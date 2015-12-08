package akka.contrib.persistence.mongodb

import com.mongodb.casbah.Imports._

/**
  * Trait applied to companion object of BsonMessage subtypes that provide to provide BSON serialization.
  */
trait CasbahBsonHandler[A] {
  private[mongodb] final def readBsonUntyped(document: MongoDBObject): AnyRef =
    readBson(document).asInstanceOf[AnyRef]

  private[mongodb] final def writeBsonUntyped(t: AnyRef): MongoDBObject =
    writeBson(t.asInstanceOf[A])

  def readBson(document: MongoDBObject): A
  def writeBson(t: A): MongoDBObject
}
