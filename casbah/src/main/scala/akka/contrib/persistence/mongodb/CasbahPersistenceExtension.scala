package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection
import com.mongodb.{MongoCommandException, WriteConcern}
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration
import scala.util.Try

object CasbahPersistenceDriver {
  import MongoPersistenceDriver._
  
  def toWriteConcern(writeSafety: WriteSafety, wtimeout: Duration, fsync: Boolean): WriteConcern = (writeSafety,wtimeout.toMillis.toInt,fsync) match {
    case (Unacknowledged,w,f) => new WriteConcern(0, w, f)
    case (Acknowledged,w,f) => new WriteConcern(1, w, f)
    case (Journaled,w,_) => new WriteConcern(1,w,false,true)
    case (ReplicaAcknowledged,w,f) => WriteConcern.majorityWriteConcern(w,f,!f)
  }
}

class CasbahMongoDriver(system: ActorSystem, config: Config) extends MongoPersistenceDriver(system, config) {
  import akka.contrib.persistence.mongodb.CasbahPersistenceDriver._
  
  // Collection type
  type C = MongoCollection

  type D = DBObject

  override private[mongodb] def closeConnections(): Unit = client.close()

  override private[mongodb] def upgradeJournalIfNeeded(): Unit = {
    import scala.collection.immutable.{Seq => ISeq}
    import CasbahSerializers._

    val j = collection(journalCollectionName)
    val q = MongoDBObject(VERSION -> MongoDBObject("$exists" -> 0))
    val legacyClusterSharding = MongoDBObject(PROCESSOR_ID -> s"^/user/sharding/[^/]+Coordinator/singleton/coordinator".r )

    Try(j.remove(legacyClusterSharding)).map(
      wr => logger.info(s"Removed ${wr.getN} legacy cluster sharding records as part of upgrade")
    ).recover {
      case x => logger.error("Exception occurred while removing legacy cluster sharding records",x)
    }

    Try(j.dropIndex(MongoDBObject(PROCESSOR_ID -> 1, SEQUENCE_NUMBER -> 1, DELETED -> 1))).map(
      _ => logger.info("Successfully dropped legacy index")
    ).recover {
      case e:MongoCommandException if e.getErrorMessage.startsWith("index not found with name") =>
        logger.info("Legacy index has already been dropped")
      case t =>
        logger.error("Received error while dropping legacy index",t)
    }

    val cnt = j.count(q)
    logger.info(s"Journal automatic upgrade found $cnt records needing upgrade")
    if(cnt > 0) {
      val results = j.find[DBObject](q)
       .map(d => d.as[ObjectId]("_id") -> Event[DBObject](useLegacySerialization)(deserializeJournal(d).toRepr, d.getAs[Long](GLOBAL_SEQUENCE_NUMBER).getOrElse(0L)))
       .map{case (id,ev) => j.update("_id" $eq id, serializeJournal(Atom(ev.pid, ev.sn, ev.sn, ev.gsn, ev.gsn, ISeq(ev))))}
      results.foldLeft((0, 0)) { case ((successes, failures), result) =>
        val n = result.getN
        if (n > 0)
          (successes + n) -> failures
        else
          successes -> (failures + 1)
      } match {
        case (s,f) if f > 0 =>
          logger.warn(s"There were $s successful updates and $f failed updates")
        case (s,_) =>
          logger.info(s"$s records were successfully updated")
      }

    }
  }

  private[this] val url = MongoClientURI(mongoUri)

  private[mongodb] lazy val client = MongoClient(url)

  private[mongodb] lazy val db = client(databaseName.getOrElse(url.database.getOrElse(DEFAULT_DB_NAME)))

  private[mongodb] def collection(name: String) = db(name)
  private[mongodb] def journalWriteConcern: WriteConcern = toWriteConcern(journalWriteSafety,journalWTimeout,journalFsync)
  private[mongodb] def snapsWriteConcern: WriteConcern = toWriteConcern(snapsWriteSafety,snapsWTimeout,snapsFsync)


  private[mongodb] override def ensureUniqueIndex(collection: C, indexName: String, keys: (String,Int)*)(implicit ec: ExecutionContext): MongoCollection = {
    collection.createIndex(
      MongoDBObject(keys :_*),
      MongoDBObject("unique" -> true, "name" -> indexName))
    collection
  }
}

class CasbahPersistenceExtension(val actorSystem: ActorSystem) extends MongoPersistenceExtension {

  override def configured(config: Config): Configured = Configured(config)

  case class Configured(config: Config) extends ConfiguredExtension {

    val driver = new CasbahMongoDriver(actorSystem, config)

    override lazy val journaler = new CasbahPersistenceJournaller(driver) with MongoPersistenceJournalMetrics with MongoPersistenceJournalFailFast {
      override def driverName = "casbah"
      override private[mongodb] val breaker = driver.breaker
    }
    override lazy val snapshotter = new CasbahPersistenceSnapshotter(driver) with MongoPersistenceSnapshotFailFast {
      override private[mongodb] val breaker = driver.breaker
    }
    override lazy val readJournal = new CasbahPersistenceReadJournaller(driver)
  }

}
