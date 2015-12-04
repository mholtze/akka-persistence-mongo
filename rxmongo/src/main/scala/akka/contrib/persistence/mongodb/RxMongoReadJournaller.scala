package akka.contrib.persistence.mongodb

import akka.actor.{Status, Stash, Actor, Props}
import akka.persistence.query.EventEnvelope
import akka.stream.actor.ActorPublisher
import play.api.libs.iteratee.{Concurrent, Enumeratee, Iteratee, Enumerator}
import reactivemongo.api.commands.Command
import reactivemongo.api.{BSONSerializationPack, QueryOpts}
import reactivemongo.bson._

trait IterateeActorPublisher[T] extends ActorPublisher[T] with Stash {

  import akka.pattern.pipe
  import akka.stream.actor.ActorPublisherMessage._
  import context.dispatcher

  def initial: Enumerator[T]

  override def preStart() = {
    context.become(streaming(initial andThen Enumerator.eof[T] through Enumeratee.onEOF(onCompleteThenStop)))
  }

  override def receive: Receive = Actor.emptyBehavior

  case class Continue(enm: Enumerator[T])
  case class Failure(t: Throwable)

  def streaming(enumerator: Enumerator[T]): Receive = {
    case _:Cancel|SubscriptionTimeoutExceeded =>
      onCompleteThenStop()
    case Request(_) =>
      Concurrent.runPartial(enumerator,next).map {
        case (_,enm) => Continue(enm)
      }.pipeTo(self)
      context.become(publishing)
  }

  def publishing: Receive = {
    case Continue(enm) =>
      context.become(streaming(enm))
      unstashAll()
    case Status.Failure(t) =>
      onErrorThenStop(t)
    case x =>
      stash()
  }

  private val onNextIteratee = Iteratee.foreach[T](onNext)
  private def next = {
    Enumeratee take totalDemand.toIntWithoutWrapping transform onNextIteratee
  }

}

object AllEvents {
  def props(driver: RxMongoDriver) = Props(new AllEvents(driver))
}

class AllEvents(val driver: RxMongoDriver) extends IterateeActorPublisher[EventEnvelope] {
  import RxMongoSerializers._
  import JournallingFieldNames._
  import context.dispatcher

  private val opts = QueryOpts().noCursorTimeout

  private val flatten: Enumeratee[BSONDocument,EventEnvelope] = Enumeratee.mapFlatten[BSONDocument] { doc =>
    Enumerator(
      doc.as[BSONArray](EVENTS).values.collect {
        case d:BSONDocument => driver.deserializeJournal(d)
      }.zipWithIndex.map{case (ev,idx) => ev.toEnvelope(idx.toLong)} : _*
    )
  }

  override def initial: Enumerator[EventEnvelope] = {
    driver.journal
      .find(BSONDocument())
      .options(opts)
      .sort(BSONDocument(PROCESSOR_ID -> 1, FROM -> 1))
      .projection(BSONDocument(EVENTS -> 1))
      .cursor[BSONDocument]()
      .enumerate()
      .through(flatten)
  }
}

object AllPersistenceIds {
  def props(driver: RxMongoDriver) = Props(new AllPersistenceIds(driver))
}

class AllPersistenceIds(val driver: RxMongoDriver) extends IterateeActorPublisher[String] {
  import JournallingFieldNames._
  import context.dispatcher

  private val flatten: Enumeratee[BSONDocument,String] = Enumeratee.mapFlatten[BSONDocument] { doc =>
    Enumerator(doc.getAs[Vector[String]]("values").get : _*)
  }

  override def initial = {
    val q = BSONDocument("distinct" -> driver.journalCollectionName, "key" -> PROCESSOR_ID, "query" -> BSONDocument())
    val cmd = Command.run(BSONSerializationPack)
    cmd(driver.db,cmd.rawCommand(q))
      .cursor[BSONDocument]
      .enumerate()
      .through(flatten)
  }
}

object EventsByPersistenceId {
  def props(driver:RxMongoDriver,persistenceId:String,fromSeq:Long,toSeq:Long):Props =
    Props(new EventsByPersistenceId(driver,persistenceId,fromSeq,toSeq))
}

class EventsByPersistenceId(val driver:RxMongoDriver,persistenceId:String,fromSeq:Long,toSeq:Long) extends IterateeActorPublisher[EventEnvelope] {
  import JournallingFieldNames._
  import RxMongoSerializers._
  import context.dispatcher

  private val flatten: Enumeratee[BSONDocument,EventEnvelope] = Enumeratee.mapFlatten[BSONDocument] { doc =>
    Enumerator(
      doc.as[BSONArray](EVENTS).values.collect {
        case d:BSONDocument => driver.deserializeJournal(d)
      }.zipWithIndex.map{case (ev,idx) => ev.toEnvelope(idx.toLong)} : _*
    )
  }

  private val filter = Enumeratee.filter[EventEnvelope] { e =>
    e.sequenceNr >= fromSeq && e.sequenceNr <= toSeq
  }

  override def initial = {
    val q = BSONDocument(
      PROCESSOR_ID -> persistenceId,
      FROM -> BSONDocument("$gte" -> fromSeq),
      FROM -> BSONDocument("$lte" -> toSeq)
    )
    driver.journal.find(q)
      .sort(BSONDocument(PROCESSOR_ID -> 1, FROM -> 1))
      .projection(BSONDocument(EVENTS -> 1))
      .cursor[BSONDocument]()
      .enumerate()
      .through(flatten)
      .through(filter)
  }
}

class RxMongoReadJournaller(driver: RxMongoDriver) extends MongoPersistenceReadJournallingApi {
  override def allPersistenceIds: Props = AllPersistenceIds.props(driver)

  override def allEvents: Props = AllEvents.props(driver)

  override def eventsByPersistenceId(persistenceId: String, fromSeq: Long, toSeq: Long): Props =
    EventsByPersistenceId.props(driver,persistenceId,fromSeq,toSeq)

  override def allEventsInGlobalOrder(globalFrom: Long, globalTo: Long): Props =
    AllEventsInGlobalOrder.props(driver, globalFrom, globalTo)
}

object AllEventsInGlobalOrder {
  def props(driver: RxMongoDriver, globalFrom: Long, globalTo: Long) = Props(new AllEventsInGlobalOrder(driver, globalFrom, globalTo))
}

class AllEventsInGlobalOrder(val driver: RxMongoDriver, globalFrom: Long, globalTo: Long) extends IterateeActorPublisher[GlobalEventEnvelope] {
  import RxMongoSerializers._
  import JournallingFieldNames._
  import context.dispatcher

  private val opts = QueryOpts().noCursorTimeout

  private val flatten: Enumeratee[BSONDocument,GlobalEventEnvelope] = Enumeratee.mapFlatten[BSONDocument] { doc =>
    Enumerator(
      doc.as[BSONArray](EVENTS).values.collect {
        case d:BSONDocument => driver.deserializeJournal(d)
      }.zipWithIndex.map{case (ev,idx) => ev.toGlobalEnvelope(idx.toLong)} : _*
    )
  }

  private val filter = Enumeratee.filter[GlobalEventEnvelope] { e =>
    e.globalSequenceNr >= globalFrom && e.globalSequenceNr <= globalTo
  }

  override def initial: Enumerator[GlobalEventEnvelope] = {
    val q = BSONDocument(
      GLOBAL_FROM -> BSONDocument("$gte" -> globalFrom),
      GLOBAL_FROM -> BSONDocument("$lte" -> globalTo)
    )
    driver.journal
      .find(q)
      .options(opts)
      .sort(BSONDocument(GLOBAL_FROM -> 1))
      .projection(BSONDocument(EVENTS -> 1))
      .cursor[BSONDocument]()
      .enumerate()
      .through(flatten)
      .through(filter)
  }
}