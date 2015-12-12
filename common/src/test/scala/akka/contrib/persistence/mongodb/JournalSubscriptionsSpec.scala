package akka.contrib.persistence.mongodb

import akka.actor._
import akka.persistence.{Persistence, PersistentActor}
import akka.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.util.{Success, Try}

abstract class JournalSubscriptionsSpec(extensionClass: Class[_]) extends BaseUnitTest with EmbeddedMongo with BeforeAndAfterAll {

  import ConfigLoanFixture._

  override def embedDB = "load-test"

  override def beforeAll(): Unit = {
    doBefore()
  }

  override def afterAll(): Unit = {
    doAfter()
  }

  val journalPluginId = "akka-contrib-mongodb-persistence-journal"

  def config(extensionClass: Class[_]) = ConfigFactory.parseString(s"""
    |akka.contrib.persistence.mongodb.mongo.driver = "${extensionClass.getName}"
    |akka.contrib.persistence.mongodb.mongo.mongouri = "mongodb://localhost:$embedConnectionPort/$embedDB"
    |akka.contrib.persistence.mongodb.mongo.breaker.timeout.call = 0s
    |akka.persistence.journal.plugin = "akka-contrib-mongodb-persistence-journal"
    |akka-contrib-mongodb-persistence-journal {
    |	  # Class name of the plugin.
    |  class = "akka.contrib.persistence.mongodb.MongoJournal"
    |}
    |akka.persistence.snapshot-store.plugin = "akka-contrib-mongodb-persistence-snapshot"
    |akka-contrib-mongodb-persistence-snapshot {
    |	  # Class name of the plugin.
    |  class = "akka.contrib.persistence.mongodb.MongoSnapshots"
    |}
    |""".stripMargin)

  sealed trait Command
  case class Inc() extends Command
  case class IncBatch(n: Int) extends Command
  case object Stop extends Command

  sealed trait Event
  case object IncEvent extends Event

  case class CounterState(init: Int = 0) {
    var counter = init

    def event(ev: Event): CounterState = ev match {
      case IncEvent =>
        counter += 1
        this
    }
  }

  class CounterPersistentActor extends PersistentActor with ActorLogging {
    var state = CounterState(0)
    var accumulator: Option[ActorRef] = None

    override def receiveRecover: Receive = {
      case x:Int => (1 to x).foreach(_ => state.event(IncEvent))
    }

    private def eventHandler(int: Int): Unit = {
      state.event(IncEvent)
    }

    override def receiveCommand: Receive = {
      case Inc() =>
        persist(1)(eventHandler)
      case IncBatch(count) =>
        persistAll((1 to count).map(_ => 1))(eventHandler)
      case Stop =>
        context.stop(self)
    }

    override def persistenceId: String = s"counter"
  }

  class SubscriberActor(testActor: ActorRef) extends Actor with ActorLogging {
    val journal = Persistence(context.system).journalFor(journalPluginId)

    journal ! MongoJournal.SubscribeToEvents

    @throws[Exception](classOf[Exception])
    override def preStart(): Unit = {
      super.preStart()
      self ! "ready"
    }

    override def receive: Actor.Receive = {
      case "unsubscribe" => journal ! MongoJournal.UnsubscribeFromEvents
      case str: String => testActor ! str
      case msg: MongoJournal.EventsPersisted => testActor ! msg
    }
  }

  "A mongo persistence driver" should "broadcast single events" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-journal", "load-test") { case (as,config) =>
    val probe = TestProbe()(as)
    val subscriber = as.actorOf(Props(new SubscriberActor(probe.ref)), "subscriber")
    val actor = as.actorOf(Props(new CounterPersistentActor), "counter")

    probe.expectMsg(5.seconds, "ready")
    actor ! Inc()

    val eventsPersisted = probe.expectMsgClass(5.seconds, classOf[MongoJournal.EventsPersisted])
    eventsPersisted.events.size shouldBe 1
    eventsPersisted.events.head.event shouldBe 1
  }

  it should "broadcast multiple events for persistAll" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-journal", "load-test") { case (as,config) =>
    val probe = TestProbe()(as)
    val subscriber = as.actorOf(Props(new SubscriberActor(probe.ref)), "subscriber")
    val actor = as.actorOf(Props(new CounterPersistentActor), "counter")

    probe.expectMsg(5.seconds, "ready")
    actor ! IncBatch(4)

    val eventsPersisted = probe.expectMsgClass(5.seconds, classOf[MongoJournal.EventsPersisted])
    eventsPersisted.events.size shouldBe 4
    eventsPersisted.events.map(_.event.asInstanceOf[Int]) shouldBe Seq(1, 1, 1, 1)
  }

  it should "broadcast to multiple subscribers" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-journal", "load-test") { case (as,config) =>
    val probe1 = TestProbe()(as)
    val probe2 = TestProbe()(as)
    val subscriber1 = as.actorOf(Props(new SubscriberActor(probe1.ref)), "subscriber1")
    val subscriber2 = as.actorOf(Props(new SubscriberActor(probe2.ref)), "subscriber2")
    val actor = as.actorOf(Props(new CounterPersistentActor), "counter")

    probe1.expectMsg(5.seconds, "ready")
    probe2.expectMsg(5.seconds, "ready")
    actor ! Inc()

    val eventsPersisted1 = probe1.expectMsgClass(5.seconds, classOf[MongoJournal.EventsPersisted])
    val eventsPersisted2 = probe2.expectMsgClass(5.seconds, classOf[MongoJournal.EventsPersisted])
    eventsPersisted1 shouldBe eventsPersisted2
  }

  it should "handle unsubscribe" in withConfig(config(extensionClass), "akka-contrib-mongodb-persistence-journal", "load-test") { case (as,config) =>
    val probe1 = TestProbe()(as)
    val probe2 = TestProbe()(as)
    val subscriber1 = as.actorOf(Props(new SubscriberActor(probe1.ref)), "subscriber1")
    val subscriber2 = as.actorOf(Props(new SubscriberActor(probe2.ref)), "subscriber2")
    val actor = as.actorOf(Props(new CounterPersistentActor), "counter")

    probe1.expectMsg(5.seconds, "ready")
    probe2.expectMsg(5.seconds, "ready")
    actor ! Inc()

    val eventsPersisted1 = probe1.expectMsgClass(5.seconds, classOf[MongoJournal.EventsPersisted])
    val eventsPersisted2 = probe2.expectMsgClass(5.seconds, classOf[MongoJournal.EventsPersisted])
    eventsPersisted1 shouldBe eventsPersisted2

    subscriber1 ! "unsubscribe"
    actor ! Inc()
    probe2.expectMsgClass(5.seconds, classOf[MongoJournal.EventsPersisted])
    probe1.expectNoMsg(1.seconds)

  }
}
