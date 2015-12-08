package akka.contrib.persistence.mongodb

/**
  * Base trait for any message/case class that can be directly serialized to BSON. How Bson message serialization
  * is handled is driver specific.
  */
trait BsonMessage { }
