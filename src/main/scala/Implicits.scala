package com.typesafe.akka.extension.mongodb.replicator



import akka.event.LogSource

/**
 * Package imports, for things like implicits shared across the system.
 */
object `package` {
  implicit val mongoExtensionLoggerType: LogSource[MongoReplicatorExtension] = new LogSource[MongoReplicatorExtension] {
    def genString(t: MongoReplicatorExtension): String = "[" + t + "]"
  }

}