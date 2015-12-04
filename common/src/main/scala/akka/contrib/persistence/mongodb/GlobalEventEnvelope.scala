package akka.contrib.persistence.mongodb

import akka.persistence.query.EventEnvelope

/** EventEnvelope implementation that also includes the global sequence number. */
final case class GlobalEventEnvelope(
  offset: Long,
  persistenceId: String,
  sequenceNr: Long,
  globalSequenceNr: Long,
  event: Any
) {
  /** Convert to a regular event envelope that does not have a global sequence number. */
  lazy val toEventEnvelope: EventEnvelope = EventEnvelope(offset, persistenceId, sequenceNr, event)
}