package com.typesafe.akka.extension.mongodb.replicator

import akka.actor._
import akka.event.{LogSource, Logging}

import com.typesafe.config.{ConfigFactory, Config}

import java.util.{Date, TimeZone}

import scala.collection.immutable

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoClientURI
import org.bson.types.BSONTimestamp

object MongoReplicatorExtension extends ExtensionKey[MongoReplicatorExtension]

class MongoReplicatorExtension(implicit val system: ExtendedActorSystem) extends Extension {
  private val log = Logging(system, this)
  protected val config = system.settings.config.getConfig("akka.mongodb.replicator").withFallback(defaultConfig).root.toConfig

  lazy val defaultConfig = ConfigFactory.parseString(
    """
     akka.mongodb.replicator {
      isReplicaSet = true
     }
    """.stripMargin)
  /** A full MongoURI containing the server to connect to. We will ignore any DB + Collection names,
    * as those are provided at initialisation time.
    */
  val mongoURI = MongoClientURI(config.getString("uri"))

  val isRS = config.getBoolean("isReplicaSet") // if not, look for Master/Slave oplog

  log.debug("Attempting to connect to MongoDB at URI {}", mongoURI)

  private val mongo = MongoClient(mongoURI)

  /**
   * Begins replication on the given connection, sending instances of `MongoOpLogEntry` to the 'receiver' `ActorRef`.
   *
   * By default (no DB or Collection specified), replicates *ALL* dbs + collections in mongodb.
   *
   * If DB is specified, replicates all collections in that DB.
   * If Collection & DB are specified, replicates only that collection.
   *
   * YOU CANNOT SPECIFY COLLECTION W/O A DB.
   *
   * If a DB + Collection are specified, you may enable "fullResync", which will scan the
   * collection and send all entries in it as MongoInsertOperation with a -1 opID to the Actor before replication monitoring
   * starts.
   *
   * @param receiver
   * @param fullResync
   * @param db
   * @param collection
   */
  def replicate(receiver: ActorRef, db: Option[String] = None, collection: Option[String] = None, fullResync: Boolean = false) = {
    if (collection.isDefined) require(db.isDefined, "Cannot specify a collection without a db.")
    if (fullResync) require(db.isDefined && collection.isDefined, "Full Resync is only permitted with both a DB And a Collection specified.")

    val ns: Option[String] = db match {
      case Some(database) => Some(database + (collection match {
        case Some(coll) => "." + coll
        case None => ""
      }))
      case None => None
    }

    /** The replicator is a Iterator over the data in the oplog and shouldn't ever end... */
    val replicator = new MongoOpLog(mongo, startTimestamp = None, namespace = ns, replicaSet = isRS)

    val monitor = new Runnable {
      override def run() = {
        if (fullResync) {
          for (doc: DBObject <- mongo(db.get)(collection.get)) {
            val obj = new MongoInsertOperation(new BSONTimestamp, opID = Some(-1), namespace = ns.get, document = doc)
            log.debug("Constructed a pseduo Insert Entry {}", obj)
            receiver ! obj
          }
        }
        for (obj: MongoOpLogEntry <- replicator) {
          log.debug("Got an OpLogEntry '{}'", obj)
          receiver ! obj
        }

      }
    }

    akka.dispatch.ExecutionContext.defaultExecutionContext.execute(monitor)
  }


}

