package akka.contrib.persistence.mongodb

import akka.actor.{Actor, ExtendedActorSystem, Props}
import akka.persistence.query._
import akka.persistence.query.scaladsl.{CurrentEventsByPersistenceIdQuery, CurrentPersistenceIdsQuery}
import akka.persistence.query.javadsl.{CurrentEventsByPersistenceIdQuery => JCEBP, CurrentPersistenceIdsQuery => JCP}
import akka.stream.actor.{ActorPublisher, ActorPublisherMessage}
import akka.stream.scaladsl.Source
import akka.stream.javadsl.{Source => JSource}
import com.typesafe.config.Config

object MongoReadJournal {
  val Identifier = "akka-contrib-mongodb-persistence-readjournal"
}

class MongoReadJournal(system: ExtendedActorSystem, config: Config) extends ReadJournalProvider {

  private[this] val impl = MongoPersistenceExtension(system)(config).readJournal

  override def scaladslReadJournal(): scaladsl.ReadJournal = new ScalaDslMongoReadJournal(impl)

  override def javadslReadJournal(): javadsl.ReadJournal = new JavaDslMongoReadJournal(new ScalaDslMongoReadJournal(impl))
}

class ScalaDslMongoReadJournal(impl: MongoPersistenceReadJournallingApi) extends scaladsl.ReadJournal with CurrentPersistenceIdsQuery with CurrentEventsByPersistenceIdQuery {

  def allEvents(): Source[EventEnvelope,Unit] =
    Source.actorPublisher[EventEnvelope](impl.allEvents)
      .mapMaterializedValue(_ => ())

  override def currentPersistenceIds(): Source[String, Unit] =
    Source.actorPublisher[String](impl.allPersistenceIds)
      .mapMaterializedValue(_ => ())

  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): Source[EventEnvelope, Unit] = {
    require(persistenceId != null, "PersistenceId must not be null")
    Source.actorPublisher[EventEnvelope](impl.eventsByPersistenceId(persistenceId,fromSequenceNr,toSequenceNr))
      .mapMaterializedValue(_ => ())
  }

  def allEventsInGlobalOrder(globalFrom: Long, globalTo: Long): Source[GlobalEventEnvelope, Unit] =
    Source.actorPublisher[GlobalEventEnvelope](impl.allEventsInGlobalOrder(globalFrom, globalTo))
      .mapMaterializedValue(_ => ())

  def liveGlobalEvents(
    globalFrom: Option[Long] = Option.empty,
    maxBufferSize: Long = MongoGlobalEventsLiveQuery.MaxBufferSize
  ): Source[GlobalEventEnvelope, Unit] =
    Source.actorPublisher[GlobalEventEnvelope](MongoGlobalEventsLiveQuery.props(globalFrom, maxBufferSize))
      .mapMaterializedValue(_ => ())
}

class JavaDslMongoReadJournal(rj: ScalaDslMongoReadJournal) extends javadsl.ReadJournal with JCP with JCEBP {
  def allEvents(): JSource[EventEnvelope, Unit] = rj.allEvents().asJava

  override def currentPersistenceIds(): JSource[String, Unit] = rj.currentPersistenceIds().asJava

  override def currentEventsByPersistenceId(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long): JSource[EventEnvelope, Unit] = {
    require(persistenceId != null, "PersistenceId must not be null")
    rj.currentEventsByPersistenceId(persistenceId, fromSequenceNr, toSequenceNr).asJava
  }

  def allEventsInGlobalOrder(globalFrom: Long, globalTo: Long): JSource[GlobalEventEnvelope, Unit] =
    rj.allEventsInGlobalOrder(globalFrom, globalTo).asJava
}

trait MongoPersistenceReadJournallingApi {
  def allPersistenceIds: Props
  def allEvents: Props
  def eventsByPersistenceId(persistenceId: String, fromSeq: Long, toSeq: Long): Props
  def allEventsInGlobalOrder(globalFrom: Long, globalTo: Long): Props
}

trait SyncActorPublisher[A,Cursor] extends ActorPublisher[A] {
  import ActorPublisherMessage._

  override def preStart() = {
    context.become(streaming(initialCursor, 0))
    super.preStart()
  }

  protected def driver: MongoPersistenceDriver

  protected def initialCursor: Cursor

  protected def next(c: Cursor, atMost: Long): (Vector[A], Cursor)

  protected def isCompleted(c: Cursor): Boolean

  protected def discard(c: Cursor): Unit

  def receive = Actor.emptyBehavior

  def streaming(cursor: Cursor, offset: Long): Receive = {
    case _: Cancel | SubscriptionTimeoutExceeded =>
      discard(cursor)
      context.stop(self)
    case Request(_) =>
      val (filled,remaining) = next(cursor, totalDemand)
      filled foreach onNext
      if (isCompleted(remaining))
        onCompleteThenStop()
      else
        context.become(streaming(remaining, offset + filled.size))
  }
}