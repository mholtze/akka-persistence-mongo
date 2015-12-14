package akka.contrib.persistence.mongodb

import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.persistence.PersistentActor
import akka.persistence.query.PersistenceQuery
import akka.stream.ActorMaterializer
import akka.stream.actor.{MaxInFlightRequestStrategy, ActorSubscriberMessage, ActorSubscriber}
import akka.stream.scaladsl.Sink
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.{Await, Future, Promise}

class SlowConsumer(probe: ActorRef) extends ActorSubscriber {
  import ActorSubscriberMessage._
  import SlowConsumer._

  val MaxQueueSize = 2
  var queue = Vector[GlobalEventEnvelope]()

  override val requestStrategy = new MaxInFlightRequestStrategy(max = MaxQueueSize) {
    override def inFlightInternally: Int = queue.size
  }

  def receive = {
    case OnNext(e: GlobalEventEnvelope) =>
      queue :+= e
      probe ! queue.size
    case ReadQueued(num) =>
      val (use, keep) = queue.splitAt(num)
      queue = keep
      use.foreach(probe ! _.event)
      probe ! queue.size
      request(requestStrategy.requestDemand(remainingRequested))
  }
}

object SlowConsumer {
  case class ReadQueued(num: Int)
}

abstract class LiveGlobalEventsSpec[A <: MongoPersistenceExtension](extensionClass: Class[A]) extends BaseUnitTest with EmbeddedMongo with BeforeAndAfterAll with Eventually {

  import ConfigLoanFixture._

  override def embedDB = "live-global-query-test"

  override def beforeAll(): Unit = {
    doBefore()
  }

  override def afterAll(): Unit = {
    doAfter()
  }

  def config(extensionClass: Class[_]) = ConfigFactory.parseString(s"""
    |akka.contrib.persistence.mongodb.mongo.driver = "${extensionClass.getName}"
    |akka.contrib.persistence.mongodb.mongo.journal-read-fill-limit = 10
    |akka.contrib.persistence.mongodb.mongo.mongouri = "mongodb://localhost:$embedConnectionPort/$embedDB"
    |akka.persistence.journal.plugin = "akka-contrib-mongodb-persistence-journal"
    |akka-contrib-mongodb-persistence-journal {
    |    # Class name of the plugin.
    |  class = "akka.contrib.persistence.mongodb.MongoJournal"
    |}
    |akka.persistence.snapshot-store.plugin = "akka-contrib-mongodb-persistence-snapshot"
    |akka-contrib-mongodb-persistence-snapshot {
    |    # Class name of the plugin.
    |  class = "akka.contrib.persistence.mongodb.MongoSnapshots"
    |}
    |akka-contrib-mongodb-persistence-readjournal {
    |  # Class name of the plugin.
    |  class = "akka.contrib.persistence.mongodb.MongoReadJournal"
    |}
    |""".stripMargin).withFallback(ConfigFactory.defaultReference())

  def props(id: String, promise: Promise[Unit]) = Props(new Persistent(id, promise))

  case class Append(s: String)

  class Persistent(val persistenceId: String, completed: Promise[Unit]) extends PersistentActor {
    var events = Vector.empty[String]

    override def receiveRecover: Receive = {
      case s: String => events = events :+ s
    }

    override def receiveCommand: Receive = {
      case Append(s) => persist(s){str =>
        events = events :+ str
        if (str == "END") {
          completed.success(())
          self ! PoisonPill
        }
      }
    }
  }

  def mongoExt(as: ActorSystem) = MongoPersistenceExtension(as)(config(extensionClass))

  "Global live query" should "resend all old events" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") { case (as,_) =>
    import concurrent.duration._
    implicit val system = as
    implicit val mat = ActorMaterializer()
    val initialGlobalSeqNr = Await.result(mongoExt(as).journaler.readHighestGlobalSeqNr, 10.seconds) + 1
    val events = ("this" :: "is" :: "just" :: "a" :: "test" :: "END" :: Nil) map Append.apply

    val promise = Promise[Unit]()
    val ar = as.actorOf(props("foo",promise))

    events foreach (ar ! _)
    Await.result(promise.future, 10.seconds)

    val readJournal = PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)
    val fut = readJournal.liveGlobalEvents(Option(initialGlobalSeqNr)).take(events.size).runFold(Seq.empty[GlobalEventEnvelope]){ (received, ee) =>
      println(s"ee = $ee")
      received :+ ee
    }
    Await.result(fut,10.seconds).map(_.event) shouldBe ("this" :: "is" :: "just" :: "a" :: "test" :: "END" :: Nil)
  }

  it should "handle backpressure when catching up" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") { case (as,_) =>
    import concurrent.duration._
    implicit val system = as
    implicit val mat = ActorMaterializer()
    val initialGlobalSeqNr = Await.result(mongoExt(as).journaler.readHighestGlobalSeqNr, 10.seconds) + 1

    val events = ("this" :: "is" :: "just" :: "a" :: "test" :: "END" :: Nil) map Append.apply

    val promise = Promise[Unit]()
    val ar = as.actorOf(props("foo",promise))

    events foreach (ar ! _)
    Await.result(promise.future, 10.seconds)

    val readJournal = PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)
    // notice the buffer size has been constrained to 2
    val fut = readJournal.liveGlobalEvents(Option(initialGlobalSeqNr), 2L).take(events.size).runFold(Seq.empty[GlobalEventEnvelope]){ (received, ee) =>
      println(s"ee = $ee")
      received :+ ee
    }
    Await.result(fut,10.seconds).map(_.event) shouldBe ("this" :: "is" :: "just" :: "a" :: "test" :: "END" :: Nil)
  }

  it should "handle constraining resend from" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") { case (as,_) =>
    import concurrent.duration._
    implicit val system = as
    implicit val mat = ActorMaterializer()
    val initialGlobalSeqNr = Await.result(mongoExt(as).journaler.readHighestGlobalSeqNr, 10.seconds) + 1

    val events = ("this" :: "is" :: "just" :: "a" :: "test" :: "END" :: Nil) map Append.apply

    val promise = Promise[Unit]()
    val ar = as.actorOf(props("foo",promise))

    events foreach (ar ! _)
    Await.result(promise.future, 10.seconds)

    val readJournal = PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)
    val fut = readJournal.liveGlobalEvents(Option(initialGlobalSeqNr + 2L)).take(events.size-2).runFold(Seq.empty[GlobalEventEnvelope]){ (received, ee) =>
      println(s"ee = $ee")
      received :+ ee
    }
    Await.result(fut,10.seconds).map(_.event) shouldBe ("just" :: "a" :: "test" :: "END" :: Nil)
  }

  it should "relay live events" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") { case (as,_) =>
    import concurrent.duration._
    implicit val system = as
    implicit val mat = ActorMaterializer()

    val events = ("this" :: "is" :: "just" :: "a" :: "test" :: "END" :: Nil) map Append.apply

    val readJournal = PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)
    val fut = readJournal.liveGlobalEvents().take(events.size).runFold(Seq.empty[GlobalEventEnvelope]){ (received, ee) =>
      println(s"ee = $ee")
      received :+ ee
    }

    val promise = Promise[Unit]()
    val ar = as.actorOf(props("foo",promise))

    events foreach (ar ! _)
    Await.result(promise.future, 10.seconds)

    Await.result(fut,10.seconds).map(_.event) shouldBe ("this" :: "is" :: "just" :: "a" :: "test" :: "END" :: Nil)
  }

  it should "catchup and then relay live events" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") { case (as,_) =>
    import concurrent.duration._
    implicit val system = as
    implicit val mat = ActorMaterializer()
    val initialGlobalSeqNr = Await.result(mongoExt(as).journaler.readHighestGlobalSeqNr, 10.seconds) + 1

    val events1 = ("this" :: "is" :: "END" :: Nil) map Append.apply
    val events2 = ("just" :: "a" :: "test" :: "END" :: Nil) map Append.apply

    val promise1 = Promise[Unit]()
    val ar1 = as.actorOf(props("foo",promise1))
    events1 foreach (ar1 ! _)
    Await.result(promise1.future, 10.seconds)

    val readProbe = TestProbe()(as)
    val readJournal = PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)
    val fut = readJournal.liveGlobalEvents(Option(initialGlobalSeqNr)).take(events1.size + events2.size).runFold(Seq.empty[GlobalEventEnvelope]){ (received, ee) =>
      println(s"ee = $ee")
      readProbe.ref ! ee.event
      received :+ ee
    }
    readProbe.expectMsg(10.seconds, "this")
    readProbe.expectMsg(10.seconds, "is")
    readProbe.expectMsg(10.seconds, "END")

    val promise2 = Promise[Unit]()
    val ar2 = as.actorOf(props("foo2",promise2))
    events2 foreach (ar2 ! _)
    Await.result(promise2.future, 10.seconds)

    readProbe.expectMsg(10.seconds, "just")
    readProbe.expectMsg(10.seconds, "a")
    readProbe.expectMsg(10.seconds, "test")
    readProbe.expectMsg(10.seconds, "END")
    Await.result(fut,10.seconds).map(_.event) shouldBe ("this" :: "is" :: "END" :: "just" :: "a" :: "test" :: "END" :: Nil)
  }

  it should "handle backpressure from slow consumers" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-readjournal") { case (as,_) =>
    import concurrent.duration._
    implicit val system = as
    implicit val mat = ActorMaterializer()
    val initialGlobalSeqNr = Await.result(mongoExt(as).journaler.readHighestGlobalSeqNr, 10.seconds) + 1

    val events = ("this" :: "is" :: "just" :: "END" :: Nil) map Append.apply
    val events2 = ("more" :: "END" :: Nil) map Append.apply

    val probe = TestProbe()(as)
    val readJournal = PersistenceQuery(as).readJournalFor[ScalaDslMongoReadJournal](MongoReadJournal.Identifier)
    val consumer = readJournal.liveGlobalEvents().take(events.size + events2.size).runWith(Sink.actorSubscriber(Props(new SlowConsumer(probe.ref))))

    val ar = as.actorOf(props("foo",Promise[Unit]()))
    events foreach (ar ! _)

    probe.expectMsg(2.seconds, 1)
    probe.expectMsg(2.seconds, 2)
    consumer ! SlowConsumer.ReadQueued(1)
    probe.expectMsg(2.seconds, "this")
    probe.expectMsg(2.seconds, 1)
    consumer ! SlowConsumer.ReadQueued(1)
    probe.expectMsg(2.seconds, "is")
    probe.expectMsg(2.seconds, 0)
    probe.expectMsg(2.seconds, 1)
    probe.expectMsg(2.seconds, 2)
    consumer ! SlowConsumer.ReadQueued(2)
    probe.expectMsg(2.seconds, "just")
    probe.expectMsg(2.seconds, "END")
    probe.expectMsg(2.seconds, 0)

    val ar2 = as.actorOf(props("foo2",Promise[Unit]()))
    events2 foreach (ar2 ! _)
    probe.expectMsg(2.seconds, 1)
    probe.expectMsg(2.seconds, 2)
  }
}
