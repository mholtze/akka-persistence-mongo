package akka.contrib.persistence.mongodb.serialization

import java.io.NotSerializableException
import java.util.concurrent.ConcurrentHashMap

import akka.actor.{ExtendedActorSystem, Extension}
import akka.event.Logging
import com.typesafe.config.Config

import scala.collection.immutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

/**
  * BSON serialization module based on Akka Serialization. Contains methods for serialization and deserialization
  * as well as locating a Serializer for a particular class as defined in the mapping in the configuration.
  */
class BsonSerialization(val system: ExtendedActorSystem) extends Extension {

  /**
    * Tuple that represents mapping from Class to Serializer
    */
  type ClassSerializer = (Class[_], BsonSerializer)

  class Settings(val config: Config) {
    val Serializers: Map[String, String] = configToMap("akka.contrib.persistence.mongodb.mongo.serializers")
    val SerializationBindings: Map[String, String] = configToMap("akka.contrib.persistence.mongodb.mongo.serialization-bindings")

    private final def configToMap(path: String): Map[String, String] = {
      import scala.collection.JavaConverters._
      config.getConfig(path).root.unwrapped.asScala.toMap map { case (k, v) ⇒ (k -> v.toString) }
    }
  }

  val settings = new Settings(system.settings.config)
  val log = Logging(system, getClass.getName)

  /**
    * Serializes the given AnyRef according to the BsonSerialization configuration
    * to either a BSON document or an Exception if one was thrown.
    */
  def serialize[D](o: AnyRef): Try[D] = Try(findSerializerFor(o).toBsonUntyped(o)).map(_.asInstanceOf[D])

  /**
    * Deserializes the given array of bytes using the specified serializer id,
    * using the optional type hint to the Serializer.
    * Returns either the resulting object or an Exception if one was thrown.
    */
  def deserialize[D, T](document: D, serializerId: Int, clazz: Option[Class[_ <: T]]): Try[T] =
    Try {
      val serializer = try serializerByIdentity(serializerId).asInstanceOf[BsonSerializer] catch {
        case _: NoSuchElementException ⇒ throw new NotSerializableException(
          s"Cannot find BSON serializer with id [$serializerId]. The most probable reason is that the configuration entry " +
            "akka.contrib.persistence.mongodb.mongo.serializers is not in synch between the two systems.")
      }
      serializer.fromBsonUntyped(document.asInstanceOf[AnyRef], clazz).asInstanceOf[T]
    }

  /**
    * Deserializes the given array of bytes using the specified serializer id,
    * using the optional type hint to the Serializer.
    * Returns either the resulting object or an Exception if one was thrown.
    */
  def deserialize[D](document: D, serializerId: Int, manifest: String): Try[AnyRef] =
    Try {
      val serializer = try serializerByIdentity(serializerId).asInstanceOf[BsonSerializer] catch {
        case _: NoSuchElementException ⇒ throw new NotSerializableException(
          s"Cannot find BSON serializer with id [$serializerId]. The most probable reason is that the configuration entry " +
            "akka.contrib.persistence.mongodb.mongo.serializers is not in synch between the two systems.")
      }
      serializer match {
        case s2: BsonSerializerWithStringManifest[D] ⇒ s2.fromBson(document, manifest)
        case s1 ⇒
          if (manifest == "")
            s1.fromBsonUntyped(document.asInstanceOf[AnyRef], None)
          else {
            system.dynamicAccess.getClassFor[AnyRef](manifest) match {
              case Success(classManifest) ⇒
                s1.fromBsonUntyped(document.asInstanceOf[AnyRef], Some(classManifest))
              case Failure(e) ⇒
                throw new NotSerializableException(
                  s"Cannot find manifest class [$manifest] for serializer with id [$serializerId].")
            }
          }
      }
    }

  /**
    * Deserializes the given array of bytes using the specified type to look up what Serializer should be used.
    * Returns either the resulting object or an Exception if one was thrown.
    */
  def deserialize[D, T](document: D, clazz: Class[T]): Try[T] =
    Try(serializerFor(clazz).fromBsonUntyped(document.asInstanceOf[AnyRef], Some(clazz)).asInstanceOf[T])

  /**
    * Returns the BsonSerializer configured for the given object.
    *
    * Throws akka.ConfigurationException if no `serialization-bindings` is configured for the
    *   class of the object.
    */
  def findSerializerFor[D](o: AnyRef): BsonSerializer = serializerFor(o.getClass)

  /**
    * Returns the configured BsonSerializer for the given Class. The configured BsonSerializer
    * is used if the configured class `isAssignableFrom` from the `clazz`, i.e.
    * the configured class is a super class or implemented interface. In case of
    * ambiguity it is primarily using the most specific configured class,
    * and secondly the entry configured first.
    *
    * Throws java.io.NotSerializableException if no `serialization-bindings` is configured for the class.
    */
  def serializerFor[D](clazz: Class[_]): BsonSerializer =
    serializerMap.get(clazz) match {
      case null ⇒ // bindings are ordered from most specific to least specific
        def unique(possibilities: immutable.Seq[(Class[_], BsonSerializer)]): Boolean =
          possibilities.size == 1 ||
            (possibilities forall (_._1 isAssignableFrom possibilities(0)._1)) ||
            (possibilities forall (_._2 == possibilities(0)._2))

        val ser = bindings filter { _._1 isAssignableFrom clazz } match {
          case Seq() ⇒
            throw new NotSerializableException("No configured BSON serialization-bindings for class [%s]" format clazz.getName)
          case possibilities ⇒
            if (!unique(possibilities))
              log.warning("Multiple BSON serializers found for " + clazz + ", choosing first: " + possibilities)
            possibilities(0)._2.asInstanceOf[BsonSerializer]
        }
        serializerMap.putIfAbsent(clazz, ser) match {
          case null ⇒
            log.debug("Using BSON serializer[{}] for message [{}]", ser.getClass.getName, clazz.getName)
            ser
          case some ⇒ some.asInstanceOf[BsonSerializer]
        }
      case ser ⇒ ser.asInstanceOf[BsonSerializer]
    }

  /**
    * Tries to load the specified BsonSerializer by the fully-qualified name; the actual
    * loading is performed by the system’s [[akka.actor.DynamicAccess]].
    */
  def serializerOf(serializerFQN: String): Try[BsonSerializer] =
    system.dynamicAccess.createInstanceFor[BsonSerializer](serializerFQN, List(classOf[ExtendedActorSystem] -> system)) recoverWith {
      case _: NoSuchMethodException ⇒ system.dynamicAccess.createInstanceFor[BsonSerializer](serializerFQN, Nil)
    }

  /**
    * A Map of serializer from alias to implementation (class implementing akka.serialization.Serializer)
    * By default always contains the following mapping: "java" -> akka.serialization.JavaSerializer
    */
  private val serializers: Map[String, BsonSerializer] =
    for ((k: String, v: String) ← settings.Serializers) yield k -> serializerOf(v).get

  /**
    *  bindings is a Seq of tuple representing the mapping from Class to Serializer.
    *  It is primarily ordered by the most specific classes first, and secondly in the configured order.
    */
  private[mongodb] val bindings: immutable.Seq[ClassSerializer] =
    sort(for ((k: String, v: String) ← settings.SerializationBindings if v != "none")
      yield (system.dynamicAccess.getClassFor[Any](k).get, serializers(v))).to[immutable.Seq]

  /**
    * Sort so that subtypes always precede their supertypes, but without
    * obeying any order between unrelated subtypes (insert sort).
    */
  private def sort(in: Iterable[ClassSerializer]): immutable.Seq[ClassSerializer] =
    ((new ArrayBuffer[ClassSerializer](in.size) /: in) { (buf, ca) ⇒
      buf.indexWhere(_._1 isAssignableFrom ca._1) match {
        case -1 ⇒ buf append ca
        case x  ⇒ buf insert (x, ca)
      }
      buf
    }).to[immutable.Seq]

  /**
    * serializerMap is a Map whose keys is the class that is serializable and values is the serializer
    * to be used for that class.
    */
  private val serializerMap: ConcurrentHashMap[Class[_], BsonSerializer] =
    (new ConcurrentHashMap[Class[_], BsonSerializer] /: bindings) { case (map, (c, s)) ⇒ map.put(c, s); map }

  /**
    * Maps from a Serializer Identity (Int) to a Serializer instance (optimization)
    */
  val serializerByIdentity: Map[Int, BsonSerializer] =
    serializers map { case (_, v) ⇒ (v.identifier, v) }

}

