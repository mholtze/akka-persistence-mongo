package akka.contrib.persistence.mongodb

import akka.persistence._
import com.mongodb.DBObject
import com.mongodb.casbah.Imports._

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

class CasbahPersistenceJournaller(driver: CasbahMongoDriver) extends MongoPersistenceJournallingApi {

  import CasbahSerializers._

  implicit val system = driver.actorSystem

  private[this] implicit val serialization = driver.serialization
  private[this] implicit val bsonSerialization = driver.bsonSerialization
  private[this] lazy val writeConcern = driver.journalWriteConcern

  private[this] def journalRangeQuery(pid: String, from: Long, to: Long): DBObject =
    (PROCESSOR_ID $eq pid) ++ (FROM $gte from) ++ (FROM $lte to)

  private[this] def journal(implicit ec: ExecutionContext) = driver.journal

  private[mongodb] def journalRange(pid: String, from: Long, to: Long)(implicit ec: ExecutionContext): Iterator[Event] =
    journal.find(journalRangeQuery(pid, from, to))
           .flatMap(_.getAs[MongoDBList](EVENTS))
           .flatMap(lst => lst.collect { case x:DBObject => x })
           .filter(dbo => dbo.getAs[Long](SEQUENCE_NUMBER).exists(sn => sn >= from && sn <= to))
           .map(driver.deserializeJournal)

  import collection.immutable.{Seq => ISeq}

  private[this] case class PreparedWrite(atomic: AtomicWrite, document: Try[MongoDBObject])

  private[this] def globalEnvelopesFromWrite(pw: PreparedWrite): Seq[GlobalEventEnvelope] = {
    val events = pw.document.get.getAs[MongoDBList]("events").get
    pw.atomic.payload.zip(events).map {
      case (repr, event: DBObject) =>
        GlobalEventEnvelope(
          // offset is not very useful in this context, so just set to 0
          offset = 0L,
          persistenceId = repr.persistenceId,
          sequenceNr = repr.sequenceNr,
          globalSequenceNr = event.getAs[Long](GLOBAL_SEQUENCE_NUMBER).getOrElse(0L),
          event = repr.payload
        )
    }
  }

  private[mongodb] override def batchAppend(writes: ISeq[AtomicWrite], globalFrom: Long)(implicit ec: ExecutionContext):Future[ISeq[Try[Seq[GlobalEventEnvelope]]]] = Future {
    val globalSeqs = globalSequences(writes, globalFrom)
    val batch = writes.zip(globalSeqs).map(x => {
      val aw = x._1
      val range = x._2
      PreparedWrite(aw, Try(driver.serializeJournal(Atom[DBObject](aw, range.from, range.to, driver.useLegacySerialization))))
    })

    if (batch.forall(_.document.isSuccess)) {
      val bulk = journal.initializeOrderedBulkOperation
      batch.foreach(pw => bulk.insert(pw.document.get))
      bulk.execute(writeConcern)
      batch.map(t => Success(globalEnvelopesFromWrite(t)))
    } else { // degraded performance, cant batch
      batch.map(pw =>
        pw.document
          .map(serialized => journal.insert(serialized)(identity, writeConcern))
          .map(_ => globalEnvelopesFromWrite(pw))
      )
    }
  }

  def globalSequences(writes: Seq[AtomicWrite], globalFrom: Long): Seq[SeqRange] = {
    writes.view.drop(1).foldLeft(Vector(SeqRange(globalFrom, writes.head.size)))((result, aw) => {
      val last = result.last
      result :+ SeqRange(last.from + last.size, aw.size)
    })
  }

  private[mongodb] override def deleteFrom(persistenceId: String, toSequenceNr: Long)(implicit ec: ExecutionContext): Future[Unit] = Future {
    val query = journalRangeQuery(persistenceId, 0L, toSequenceNr)
    val update:DBObject = MongoDBObject(
      "$pull" -> MongoDBObject(
        EVENTS -> MongoDBObject(
          PROCESSOR_ID -> persistenceId,
          SEQUENCE_NUMBER -> MongoDBObject("$lte" -> toSequenceNr)
        )),
      "$set" -> MongoDBObject(FROM -> (toSequenceNr+1))
    )
    journal.update(query, update, upsert = false, multi = true, writeConcern)
    journal.remove($and(query, EVENTS $size 0), writeConcern)
    ()
  }

  private[mongodb] def maxSequenceNr(pid: String, from: Long)(implicit ec: ExecutionContext): Future[Long] = Future {
    val query = PROCESSOR_ID $eq pid
    val projection = MongoDBObject(TO -> 1)
    val sort = MongoDBObject(TO -> -1)
    val max = journal.find(query, projection).sort(sort).limit(1).one()
    max.getAs[Long](TO).getOrElse(0L)
  }

  private[mongodb] override def replayJournal(pid: String, from: Long, to: Long, max: Long)(replayCallback: PersistentRepr ⇒ Unit)(implicit ec: ExecutionContext) = Future {
    @tailrec
    def replayLimit(cursor: Iterator[Event], remaining: Long): Unit = if (remaining > 0 && cursor.hasNext) {
      replayCallback(cursor.next().toRepr)
      replayLimit(cursor, remaining - 1)
    }

    if (to >= from) {
      replayLimit(journalRange(pid, from, to), max)
    }
  }

  private[mongodb] override def readHighestGlobalSeqNr(implicit ec: ExecutionContext): Future[Long] = Future {
    val query = MongoDBObject()
    val projection = MongoDBObject(GLOBAL_TO -> 1)
    val sort = MongoDBObject(GLOBAL_TO -> -1)
    val max = journal.find(query, projection).sort(sort).limit(1).one()
    max.getAs[Long](GLOBAL_TO).getOrElse(0L)
  }
}