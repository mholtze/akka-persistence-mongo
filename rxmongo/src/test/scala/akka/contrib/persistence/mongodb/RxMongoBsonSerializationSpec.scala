package akka.contrib.persistence.mongodb

import akka.actor.ActorSystem
import akka.contrib.persistence.mongodb.serialization.BsonSerializationExtension
import akka.persistence.{AtomicWrite, PersistentRepr}
import akka.serialization.SerializationExtension
import akka.testkit.TestKit
import reactivemongo.bson._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.collection.immutable.{Seq => ISeq}

// fixme replicate tests for Casbah
case class BsonSzEvt(value: Int) extends BsonMessage

object BsonSzEvt extends RxMongoBsonReaderWriter[BsonSzEvt] {
  override def readBson(document: BSONDocument): BsonSzEvt = BsonSzEvt(document.getAs[Int]("value").get)
  override def writeBson(t: BsonSzEvt): BSONDocument = BSONDocument("value" -> t.value)
}

class RxMongoBsonSerializationSpec extends TestKit(ActorSystem("unit-test")) with RxMongoPersistenceSpec {
  import JournallingFieldNames._

  implicit val serialization = SerializationExtension(system)
  implicit val bsonSerialization = BsonSerializationExtension(system)

  def await[T](block: Future[T])(implicit ec: ExecutionContext) = {
    Await.result(block,3.seconds)
  }

  trait Fixture {
    val underTest = new RxMongoJournaller(driver)
    val records:List[PersistentRepr] = List(1L, 2L, 3L).zipWithIndex.map { sq =>
      PersistentRepr(payload = BsonSzEvt(sq._2), sequenceNr = sq._1, persistenceId = "unit-test")
    }
  }

  "BSON serialized payloads" should "be converted to documents" in { new Fixture { withJournal { journal =>
    val evt = BsonSzEvt(5)
    val ser = BsonSerialized[BSONDocument](evt)
    val doc = ser.document
    doc.getAs[Int]("value") shouldBe Some(5)
  } }
    () }

  it should "be deserialized back to case classes" in { new Fixture { withJournal { journal =>
    val evt = BsonSzEvt(5)
    val ser = BsonSerialized[BSONDocument](evt)
    val deserialized = ser.content.asInstanceOf[BsonSzEvt]
    deserialized shouldBe evt
  } }
    () }

  "A reactive mongo journal implementation" should "insert document journal records" in { new Fixture { withJournal { journal =>
    val inserted = for {
      inserted <- underTest.batchAppend(ISeq(AtomicWrite(records)), 101L)
      range <- journal.find(BSONDocument()).cursor[BSONDocument]().collect[List]()
      head <- journal.find(BSONDocument()).cursor().headOption
    } yield (range,head)
    val (range,head) = await(inserted)
    range should have size 1

    val recone = head.get.getAs[BSONArray](EVENTS).toStream.flatMap(_.values.collect {
      case e: BSONDocument => e
    }).head
    recone.getAs[String](PROCESSOR_ID) shouldBe Some("unit-test")
    recone.getAs[Long](SEQUENCE_NUMBER) shouldBe Some(1)
    recone.getAs[Long](GLOBAL_SEQUENCE_NUMBER) shouldBe Some(101)
    recone.getAs[String](TYPE) shouldBe Some("bser")
    recone.getAs[BSONDocument](PayloadKey) shouldBe Some(BSONDocument("value" -> 0))
    ()
  } }
    () }

}
