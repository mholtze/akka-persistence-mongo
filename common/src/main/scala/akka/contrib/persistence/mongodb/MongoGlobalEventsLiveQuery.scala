package akka.contrib.persistence.mongodb

import akka.actor.{Props, Stash, ActorRef}
import akka.persistence.Persistence
import akka.persistence.query.PersistenceQuery
import akka.stream.ActorMaterializer
import akka.stream.actor.{ActorSubscriber, ActorPublisher}
import akka.stream.scaladsl.Sink
import org.reactivestreams.{Subscription, Subscriber}

import scala.annotation.tailrec

// todo global query: exclude any non global history

class MongoGlobalEventsLiveQuery(globalFrom: Option[Long], maxBufferSize: Long
) extends ActorPublisher[GlobalEventEnvelope] with Stash {

  import akka.stream.actor.ActorPublisherMessage._
  import context._
  import MongoGlobalEventsLiveQuery._

  private implicit val materializer = ActorMaterializer()(context.system)

  private val journalPluginId = "akka-contrib-mongodb-persistence-journal"
  private val journal = Persistence(context.system).journalFor(journalPluginId)
  private var lastGlobalSequenceNr: Long = 0L
  private var buf = Vector.empty[GlobalEventEnvelope]

  private var allEventsSubscription: Option[Subscription] = Option.empty
  private var transitioningToLive: Boolean = false
  private var catchupRequested: Long = 0L

  self ! globalFrom
  journal ! MongoJournal.SubscribeToEvents

  @throws[Exception](classOf[Exception])
  override def postStop(): Unit = {
    journal ! MongoJournal.UnsubscribeFromEvents
    super.postStop()
  }

  override def receive: Receive = {
    case SubscriptionTimeoutExceeded =>
      context.stop(self)
    case Some(from: Long) =>
      // the -1 is needed since lastGlobalSequenceNr is the last processed number
      // and catchup is from lastGlobalSequenceNr + 1
      lastGlobalSequenceNr = from - 1
      startCatchup(transitionToLive = false)
    case None =>
      startLive()
  }

  private def catchup: Receive = {
    case AllEventsSubscribe(sub) =>
      allEventsSubscription = Option.apply(sub)
      requestCatchup()
    case AllEventsError(cause) =>
      stopCatchup(false)
      onError(cause)
    case AllEventsComplete =>
      stopCatchup(false)
      if (transitioningToLive)
        startLive()
      else
        transitionToLiveFromCatchup()
    case AllEventsNext(env: GlobalEventEnvelope) =>
      catchupRequested -= 1
      addToBuffer(List(env))
      requestCatchup()
    case Request(_) =>
      deliverBuf()
    case Cancel =>
      stopCatchup(true)
      context.stop(self)
    case MongoJournal.EventsPersisted(events) if transitioningToLive =>
      stash()
  }

  private def startCatchup(transitionToLive: Boolean): Unit = {
    require(allEventsSubscription.isEmpty, "All events subscription already exists")

    val readJournal = PersistenceQuery(context.system)
      .readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)

    val source = readJournal.allEventsInGlobalOrder(lastGlobalSequenceNr + 1, Long.MaxValue)
    source.runWith(Sink(new AllEventsSubscriber(self)))

    catchupRequested = 0L
    transitioningToLive = transitionToLive
    become(catchup)
  }

  private def stopCatchup(cancel: Boolean): Unit = {
    if (cancel) {
      allEventsSubscription.foreach(_.cancel())
    }
    allEventsSubscription = Option.empty
  }

  private def requestCatchup(): Unit = {
    val remainingCapacity = maxBufferSize - buf.size - catchupRequested;
    if (remainingCapacity > 0) {
      allEventsSubscription.foreach(_.request(remainingCapacity))
      catchupRequested += remainingCapacity
    }
  }

  private def live: Receive = {
    case MongoJournal.EventsPersisted(events) if buf.size >= maxBufferSize =>
      become(slowConsumer)
    case MongoJournal.EventsPersisted(events) =>
      // under certain circumstances, like testing, live events can arrive after catchup for the same event
      val eventsToSend = events.filter(_.globalSequenceNr > lastGlobalSequenceNr).toIndexedSeq
      if (addToBuffer(eventsToSend) < eventsToSend.size) {
        become(slowConsumer)
      }
    case Request(_) => deliverBuf()
    case Cancel => context.stop(self)
  }

  private def transitionToLiveFromCatchup(): Unit = {
    startCatchup(transitionToLive = true)
  }

  private def startLive(): Unit = {
    transitioningToLive = false
    unstashAll()
    become(live)
  }

  private def slowConsumer: Receive = {
    case Request(_) =>
      deliverBuf()
      if (buf.isEmpty) {
        startCatchup(transitionToLive = false)
      }
    case Cancel => context.stop(self)
  }

  private def addToBuffer(events: Seq[GlobalEventEnvelope]): Int = {
    val remaining = (maxBufferSize - buf.size + totalDemand).toInt
    buf ++= events.take(remaining)
    val added = remaining - (maxBufferSize - buf.size + totalDemand).toInt

    deliverBuf()
    added
  }

  @tailrec final def deliverBuf(): Unit =
    if (totalDemand > 0) {
      /*
       * totalDemand is a Long and could be larger than
       * what buf.splitAt can accept
       */
      if (totalDemand <= Int.MaxValue) {
        val (use, keep) = buf.splitAt(totalDemand.toInt)
        if (!use.isEmpty) {
          buf = keep
          use foreach onNext
          lastGlobalSequenceNr = use.last.globalSequenceNr
        }
      } else {
        val (use, keep) = buf.splitAt(Int.MaxValue)
        if (!use.isEmpty) {
          buf = keep
          use foreach onNext
          lastGlobalSequenceNr = use.last.globalSequenceNr
          deliverBuf()
        }
      }
    }
}

object MongoGlobalEventsLiveQuery {
  val MaxBufferSize: Long = 500
  def props(globalFrom: Option[Long] = Option.empty, maxBufferSize: Long = MaxBufferSize): Props =
      Props(new MongoGlobalEventsLiveQuery(globalFrom, maxBufferSize))

  private[mongodb] sealed trait AllEventsSubscriberMessage {}
  private[mongodb] final case class AllEventsNext(element: Any) extends AllEventsSubscriberMessage
  private[mongodb] final case class AllEventsError(cause: Throwable) extends AllEventsSubscriberMessage
  private[mongodb] final case class AllEventsSubscribe(subscription: Subscription) extends AllEventsSubscriberMessage
  private[mongodb] case object AllEventsComplete extends AllEventsSubscriberMessage
}

private[mongodb] final class AllEventsSubscriber(val impl: ActorRef) extends Subscriber[GlobalEventEnvelope] {
  import MongoGlobalEventsLiveQuery._

  override def onError(cause: Throwable): Unit = {
    impl ! AllEventsError(cause)
  }
  override def onComplete(): Unit = impl ! AllEventsComplete
  override def onNext(element: GlobalEventEnvelope): Unit = {
    impl ! AllEventsNext(element)
  }
  override def onSubscribe(subscription: Subscription): Unit = {
    impl ! AllEventsSubscribe(subscription)
  }
}