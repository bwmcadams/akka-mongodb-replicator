package com.typesafe.akka.extension.mongodb.replicator
package test

import org.specs2.runner.JUnitRunner
import org.specs2.Specification
import org.junit.runner.RunWith
import org.specs2.matcher.ThrownExpectations
import com.typesafe.config.ConfigFactory
import akka.actor._
import akka.testkit._
import akka.util.duration._
import akka.util.Duration
import java.util.concurrent.TimeUnit._
import com.mongodb.casbah.Imports._
import com.typesafe.akka.extension.mongodb.replicator.test.MongoReplicatorTest.TestReplicatorActor

/**
 * Separate base trait needed to get the non-default constructor to initialize properly,
 * see http://brianmckenna.org/blog/akka_scalacheck_specs2  (or just try moving this out of a trait and back on
 * the class)
 */
trait AkkaTestConfig extends Specification
                        with ThrownExpectations
                        with ImplicitSender { self: TestKit => }

@RunWith(classOf[JUnitRunner])
class MongoReplicatorExtensionSpec(_system: ActorSystem) extends TestKit(_system: ActorSystem) with AkkaTestConfig {
  def this() = this(ActorSystem("MongoReplicatorExtensionSpec", MongoReplicatorTest.sampleConfiguration))

  /** This is currently a hacky halfassed, manual verification test to give me some functional base to work off of. */
  def is = sequential /** parallel execution makes a mess when testing actorsystems */ ^
    "This is a specification to validate the behavior of the MongoDB Replicator in a functional manner" ^
    p^
    "The Mongo Replicator should" ^
      "Replicate an existing collection appropriately" ! replicateCollection ^
    end

  def replicateCollection = {
    val m = MongoClient()("test")("validateReplicator")

    m.drop()

    for (x <- 1 until 10) {
      val doc = MongoDBObject("x" -> x * 1.0)
      m += doc
    }


    val receiver = _system.actorOf(Props(new TestReplicatorActor))

    val replicator = MongoReplicatorExtension(_system).replicate(receiver, Some("test"), Some("validateReplicator"), true)

    println("foo")

    for (x <- 10 to 50) {
      val doc = MongoDBObject("y" -> x * 3.14)
      m += doc
    }

    for (doc <- m.find("x" $exists true)) {
      val x = doc.as[Double]("x")
      m.update(MongoDBObject("_id" -> doc("_id")), MongoDBObject("x_u" -> x  * 4.2))
    }

    for (doc <- m.find("y" $exists true)) {
      val x = doc.as[Double]("y")
      m.update(MongoDBObject("_id" -> doc("_id")), MongoDBObject("y_u" -> x  * 4.2))
    }

    Thread.sleep(10,000) // lazy manually verified test is lazy.

    replicator must not beNull

  }

}


object MongoReplicatorTest {

  class TestReplicatorActor extends Actor {
    def receive = {
      case MongoInsertOperation(tsp, Some(-1L), ns, doc) =>
        println("[REPLCLIENT] FULL RESYNC DOC: " + doc)
      case ins @ MongoInsertOperation(tsp, opId, ns, doc) =>
        println("[REPLCLIENT] Inserted Document: " + ins)
      case upd @ MongoUpdateOperation(tsp, opId, ns, doc, documentID) =>
        println("[REPLCLIENT] Updated Document with ID '%s': '%s'".format(documentID, doc))
      case del @ MongoDeleteOperation(tsp, opID, ns, doc) =>
        println("[REPLCLIENT] Deleted Document: '%s'".format(doc))
      case other =>
        throw new IllegalArgumentException("Invalid replication feed in? " + other)
    }
  }

  lazy val sampleConfiguration = { ConfigFactory.parseString(
    """
    akka {
      loglevel = "INFO"

      mongodb.replicator {
        uri = "mongodb://localhost:27017"
      }
    }
    """.stripMargin)
  }
}