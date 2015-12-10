package akka.contrib.persistence.mongodb

import akka.persistence._
import play.api.libs.iteratee.{Iteratee, Enumeratee, Enumerator}
import reactivemongo.api.commands.WriteResult
import reactivemongo.bson._
import DefaultBSONHandlers._

import scala.collection.immutable.{Seq => ISeq}
import scala.concurrent._
import scala.util.{Failure, Try, Success}

class RxMongoJournaller(driver: RxMongoDriver) extends MongoPersistenceJournallingApi {

  import RxMongoSerializers._
  import JournallingFieldNames._

  private[this] implicit val serialization = driver.serialization
  private[this] implicit val bsonSerialization = driver.bsonSerialization
  private[this] lazy val writeConcern = driver.journalWriteConcern

  private[this] def journal(implicit ec: ExecutionContext) = driver.journal

  private[this] def journalRangeQuery(pid: String, from: Long, to: Long) = BSONDocument(
    PROCESSOR_ID -> pid,
    FROM -> BSONDocument("$gte" -> from),
    FROM -> BSONDocument("$lte" -> to)
  )

  private[mongodb] def journalRange(pid: String, from: Long, to: Long)(implicit ec: ExecutionContext) = {
    val enum = journal.find(journalRangeQuery(pid, from, to))
                      .projection(BSONDocument(EVENTS -> 1))
                      .cursor[BSONDocument]()
                      .enumerate()
                      .flatMap(d => Enumerator(
                        d.as[BSONArray](EVENTS).values.collect {
                          case d:BSONDocument => driver.deserializeJournal(d)
                        }.toSeq : _*))
                      .through(Enumeratee.filter[Event](ev => ev.sn >= from && ev.sn <= to))
    enum.run(Iteratee.getChunks[Event])
  }

  private[this] case class PreparedWrite(atomic: AtomicWrite, document: Try[BSONDocument])

  private[this] def globalEnvelopesFromWrite(pw: PreparedWrite): Seq[GlobalEventEnvelope] = {
    val events = pw.document.get.getAs[BSONArray]("events").get.values
    pw.atomic.payload.zip(events).map {
      case (repr, event: BSONDocument) =>
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

  private[this] def writeResultToGlobalEnvelope(pw: PreparedWrite, wr: WriteResult): Try[Seq[GlobalEventEnvelope]] = {
    if (wr.ok) Success(globalEnvelopesFromWrite(pw))
    else throw wr
  }

  private[mongodb] override def batchAppend(writes: ISeq[AtomicWrite], globalFrom: Long)(implicit ec: ExecutionContext):Future[ISeq[Try[Seq[GlobalEventEnvelope]]]] = {
    val writesStream = writes.toStream
    val globalSeqs = globalSequences(writesStream, globalFrom)
    val batch = writesStream.zip(globalSeqs).map(x => {
      val aw = x._1
      val range = x._2
      PreparedWrite(aw, Try(driver.serializeJournal(Atom[BSONDocument](aw, range.from, range.to, driver.useLegacySerialization))))
    })
    Future.sequence(batch.map(pw => pw.document match {
      case Success(document:BSONDocument) => journal.insert(document, writeConcern).map(writeResultToGlobalEnvelope(pw, _))
      case f:Failure[_] => Future.successful(Failure[Seq[GlobalEventEnvelope]](f.exception))
    }))
  }

  private[this] def globalSequences(writes: Stream[AtomicWrite], globalFrom: Long): Stream[SeqRange] = writes match {
    case ISeq() => Stream.empty[SeqRange]
    case _ => {
      val sz = writes.head.size
      SeqRange(globalFrom, sz) #:: globalSequences(writes.tail, globalFrom + sz)
    }
  }

  private[mongodb] override def deleteFrom(persistenceId: String, toSequenceNr: Long)(implicit ec: ExecutionContext) = {
    val query = journalRangeQuery(persistenceId, 0L, toSequenceNr)
    val update = BSONDocument(
      "$pull" -> BSONDocument(
        EVENTS -> BSONDocument(
          PROCESSOR_ID -> persistenceId,
          SEQUENCE_NUMBER -> BSONDocument("$lte" -> toSequenceNr)
        )),
      "$set" -> BSONDocument(FROM -> (toSequenceNr + 1))
    )
    val remove = BSONDocument("$and" ->
      BSONArray(
        BSONDocument(PROCESSOR_ID -> persistenceId),
        BSONDocument(EVENTS -> BSONDocument("$size" -> 0))
      ))
    for {
      wr <- journal.update(query, update, writeConcern, upsert = false, multi = true)
        if wr.ok
      wr <- journal.remove(remove, writeConcern)
    } yield ()
  }

  private[mongodb] override def maxSequenceNr(pid: String, from: Long)(implicit ec: ExecutionContext) =
    journal.find(BSONDocument(PROCESSOR_ID -> pid))
      .projection(BSONDocument(TO -> 1))
      .sort(BSONDocument(TO -> -1))
      .cursor[BSONDocument]()
      .headOption
      .map(d => d.flatMap(_.getAs[Long](TO)).getOrElse(0L))

  private[mongodb] override def replayJournal(pid: String, from: Long, to: Long, max: Long)(replayCallback: PersistentRepr ⇒ Unit)(implicit ec: ExecutionContext) =
    if (max == 0L) Future.successful(())
    else {
      val maxInt = max.toIntWithoutWrapping
      journalRange(pid, from, to).map(_.take(maxInt).map(_.toRepr).foreach(replayCallback))
    }

  private[mongodb] override def readHighestGlobalSeqNr(implicit ec: ExecutionContext): Future[Long] = {
    journal
      .find(BSONDocument())
      .projection(BSONDocument(GLOBAL_TO -> 1))
      .sort(BSONDocument(GLOBAL_TO -> -1))
      .cursor[BSONDocument]()
      .headOption
      .map(d => d.flatMap(_.getAs[Long](GLOBAL_TO)).getOrElse(0L))
  }
}