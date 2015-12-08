package akka.contrib.persistence.mongodb.serialization

import akka.actor.ExtendedActorSystem

/**
  * A Serializer represents a bimap between an object and a driver-specific BSON document. This is
  * based on Akka Serialization.
  *
  * Serializers are loaded using reflection during [[akka.actor.ActorSystem]]
  * start-up, where two constructors are tried in order:
  *
  * <ul>
  * <li>taking exactly one argument of type [[akka.actor.ExtendedActorSystem]];
  * this should be the preferred one because all reflective loading of classes
  * during deserialization should use ExtendedActorSystem.dynamicAccess (see
  * [[akka.actor.DynamicAccess]]), and</li>
  * <li>without arguments, which is only an option if the serializer does not
  * load classes using reflection.</li>
  * </ul>
  *
  * <b>Be sure to always use the [[akka.actor.DynamicAccess]] for loading classes!</b> This is necessary to
  * avoid strange match errors and inequalities which arise from different class loaders loading
  * the same class.
  */
trait BsonSerializer {
  /**
    * Completely unique value to identify this implementation of Serializer, used to optimize network traffic.
    * Values from 0 to 16 are reserved for Akka internal usage.
    */
  def identifier: Int

  /**
    * Returns whether this serializer needs a manifest in the fromBinary method
    */
  def includeManifest: Boolean

  /**
    * Serializes the given object into a BSON document
    */
  def toBsonUntyped(o: AnyRef): AnyRef

  /**
    * Produces an object from a BSON document, with an optional type-hint;
    * the class should be loaded using ActorSystem.dynamicAccess.
    */
  def fromBsonUntyped(document: AnyRef, manifest: Option[Class[_]]): AnyRef
}

/**
  * A Serializer represents a bimap between an object and a driver-specific BSON document. This is
  * based on Akka Serialization.
  *
  * For serialization of data that need to evolve over time the `SerializerWithStringManifest` is recommended instead
  * of [[BsonSerializer]] because the manifest (type hint) is a `String` instead of a `Class`. That means
  * that the class can be moved/removed and the serializer can still deserialize old data by matching
  * on the `String`. This is especially useful for Akka Persistence.
  *
  * The manifest string can also encode a version number that can be used in [[#fromBson]] to
  * deserialize in different ways to migrate old data to new domain objects.
  *
  * If the data was originally serialized with [[BsonSerializer]] and in a later version of the
  * system you change to `SerializerWithStringManifest` the manifest string will be the full class name if
  * you used `includeManifest=true`, otherwise it will be the empty string.
  *
  * Serializers are loaded using reflection during [[akka.actor.ActorSystem]]
  * start-up, where two constructors are tried in order:
  *
  * <ul>
  * <li>taking exactly one argument of type [[akka.actor.ExtendedActorSystem]];
  * this should be the preferred one because all reflective loading of classes
  * during deserialization should use ExtendedActorSystem.dynamicAccess (see
  * [[akka.actor.DynamicAccess]]), and</li>
  * <li>without arguments, which is only an option if the serializer does not
  * load classes using reflection.</li>
  * </ul>
  *
  * <b>Be sure to always use the [[akka.actor.DynamicAccess]] for loading classes!</b> This is necessary to
  * avoid strange match errors and inequalities which arise from different class loaders loading
  * the same class.
  */
abstract class BsonSerializerWithStringManifest[D] extends BsonSerializer {

  /**
    * Completely unique value to identify this implementation of Serializer, used to optimize network traffic.
    * Values from 0 to 16 are reserved for Akka internal usage.
    */
  def identifier: Int

  final override def includeManifest: Boolean = true

  /**
    * Return the manifest (type hint) that will be provided in the fromBinary method.
    * Use `""` if manifest is not needed.
    */
  def manifest(o: AnyRef): String

  /**
    * Serializes the given object into an Array of Byte
    */
  def toBson(o: AnyRef): D

  /**
    * Produces an object from a BSON document, with an optional type-hint;
    * the class should be loaded using ActorSystem.dynamicAccess.
    */
  def fromBson(dcoument: D, manifest: String): AnyRef

  final def fromBson(dcoument: D, manifest: Option[Class[_]]): AnyRef = {
    val manifestString = manifest match {
      case Some(c) ⇒ c.getName
      case None    ⇒ ""
    }
    fromBson(dcoument, manifestString)
  }

  final def toBsonUntyped(o: AnyRef): AnyRef =
    toBson(o).asInstanceOf[AnyRef]

  final def fromBsonUntyped(document: AnyRef, manifest: Option[Class[_]]): AnyRef =
    fromBson(document.asInstanceOf[D], manifest)

}

/**
  *  Base serializer trait with serialization identifiers configuration contract,
  *  when globally unique serialization identifier is configured in the `reference.conf`.
  */
trait BaseBsonSerializer[D] extends BsonSerializer {
  /**
    *  Actor system which is required by most serializer implementations.
    */
  def system: ExtendedActorSystem
  /**
    * Configuration namespace of serialization identifiers in the `reference.conf`.
    *
    * Each serializer implementation must have an entry in the following format:
    * `akka.contrib.persistence.mongodb.mongo.serialization-identifiers."FQCN" = ID`
    * where `FQCN` is fully qualified class name of the serializer implementation
    * and `ID` is globally unique serializer identifier number.
    */
  final val SerializationIdentifiers = "akka.contrib.persistence.mongodb.mongo.serialization-identifiers"
  /**
    * Globally unique serialization identifier configured in the `reference.conf`.
    *
    * See [[BsonSerializer#identifier()]].
    */
  override val identifier: Int = identifierFromConfig

  /**
    * Serializes the given object into a BSON document
    */
  def toBson(o: AnyRef): D

  /**
    * Produces an object from a BSON document, with an optional type-hint;
    * the class should be loaded using ActorSystem.dynamicAccess.
    */
  def fromBson(document: D, manifest: Option[Class[_]]): AnyRef

  /**
    * INTERNAL API
    */
  private[mongodb] def identifierFromConfig: Int =
    system.settings.config.getInt(s"""${SerializationIdentifiers}."${getClass.getName}"""")

  final def toBsonUntyped(o: AnyRef): AnyRef =
    toBson(o).asInstanceOf[AnyRef]

  final def fromBsonUntyped(document: AnyRef, manifest: Option[Class[_]]): AnyRef =
    fromBson(document.asInstanceOf[D], manifest)
}

