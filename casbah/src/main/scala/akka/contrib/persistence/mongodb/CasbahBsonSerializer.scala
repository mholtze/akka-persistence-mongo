package akka.contrib.persistence.mongodb

import java.util.concurrent.atomic.AtomicReference

import akka.actor.ExtendedActorSystem
import akka.contrib.persistence.mongodb.serialization.BaseBsonSerializer
import com.mongodb.casbah.Imports._

/**
  * This Serializer serializes `akka.contrib.persistence.mongodb.BsonMessage`.
  * It is using reflection to find the companion object which must implement
  * `akka.contrib.persistence.mongodb.CasbahBsonHandler`.
  */
class CasbahBsonSerializer(val system: ExtendedActorSystem) extends BaseBsonSerializer[MongoDBObject] {

  private val bsonHandlerRef = new AtomicReference[Map[Class[_], CasbahBsonHandler[_]]](Map.empty)

  private def companionAsHandler(clazz: Class[_]): CasbahBsonHandler[_] = {
    val companionName = clazz.getName + "$"
    val companionClass = clazz.getClassLoader.loadClass(companionName)
    val moduleField = companionClass.getField("MODULE$")
    val module = moduleField.get(null)
    return module.asInstanceOf[CasbahBsonHandler[_]]
  }

  private def companionHandler(clazz: Class[_]): CasbahBsonHandler[_] = {
    val bsonHandlers = bsonHandlerRef.get()
    bsonHandlers.get(clazz) match {
      case Some(handler) => handler
      case None =>
        val uncached = companionAsHandler(clazz)
        if (bsonHandlerRef.compareAndSet(bsonHandlers, bsonHandlers.updated(clazz, uncached)))
          uncached
        else
          companionHandler(clazz)
    }
  }

  private def getHandler(manifest: Option[Class[_]]): CasbahBsonHandler[_] = {
    manifest match {
      case Some(clazz) ⇒ companionHandler(clazz)
      case None ⇒ throw new IllegalArgumentException("Need a BsonMessage class to be able to serialize BSON")
    }
  }
  override def includeManifest: Boolean = true

  override def fromBson(document: MongoDBObject, manifest: Option[Class[_]]): AnyRef = {
    val handler = getHandler(manifest)
    handler.readBsonUntyped(document)
  }

  override def toBson(obj: AnyRef): MongoDBObject = {
    val handler = getHandler(Option(obj.getClass))
    handler.writeBsonUntyped(obj)
  }
}
