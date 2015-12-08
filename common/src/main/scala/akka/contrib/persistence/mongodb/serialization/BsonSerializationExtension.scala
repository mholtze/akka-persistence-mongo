package akka.contrib.persistence.mongodb.serialization

import akka.actor.{ActorSystem, ExtendedActorSystem, ExtensionId, ExtensionIdProvider}

/**
  * BsonSerializationExtension is an Akka Extension to interact with the BsonSerialization.
  */
object BsonSerializationExtension extends ExtensionId[BsonSerialization] with ExtensionIdProvider {
  override def get(system: ActorSystem): BsonSerialization = super.get(system)
  override def lookup = BsonSerializationExtension
  override def createExtension(system: ExtendedActorSystem): BsonSerialization = new BsonSerialization(system)
}