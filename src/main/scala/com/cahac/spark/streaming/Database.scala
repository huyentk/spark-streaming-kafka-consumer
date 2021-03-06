//package com.cahac.spark.streaming
//
//import com.ibm.couchdb._
//import scalaz._
//import scalaz.concurrent.Task
//import org.slf4j.LoggerFactory
//
//object Database extends App {
//  private val logger = LoggerFactory.getLogger(Database.getClass)
//
//  // Define a simple case class to represent our data model
//  case class Person(name: String, age: Int)
//
//  // Define a type mapping used to transform class names into the doc kind
//  val typeMapping = TypeMapping(classOf[Person] -> "Person")
//  // Define some sample data
//  val alice = Person("Alice", 25)
//  val bob = Person("Bob", 30)
//  val carl = Person("Carl", 20)
//
//  // Create a CouchDB client instance
//  val couch = CouchDb("127.0.0.1", 5984, https = false, "huyentk", "Huyen1312")
//  // Define a database name
//  val dbName = "scala"
//  // Get an instance of the DB API by name and type mapping
//  val db = couch.db(dbName, typeMapping)
//
//  val actions = for {
//    _ <- couch.dbs.delete(dbName).ignoreError
//    _ <- couch.dbs.create(dbName)
//    _ <- db.docs.create(alice)
//    docs <- db.docs.getMany.includeDocs[Person].build.query
//  } yield docs.getDocsData
//
//  // Execute the actions and process the result
//  actions.unsafePerformSyncAttempt match {
//    // In case of an error (left side of Either), print it
//    case -\/(e) => logger.error(e.getMessage, e)
//    // In case of a success (right side of Either), print each object
//    case \/-(a) => a.foreach(x => logger.info(x.toString))
//  }
//  couch.client.client.shutdownNow()
//
//}
