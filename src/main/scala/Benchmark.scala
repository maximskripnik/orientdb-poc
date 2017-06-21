import util._

import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

object Benchmark extends App {

  if (args.isEmpty) {
    println("Usage: sbt run <N> where N is the number of vertices you want to load orientDB with in each of the tests")
    System.exit(1)
  }

  val n = args(0).toInt

  println("Configurations read:\n" +
    s"Complexity count: $n, " +
    s"Database url: $dbUrl, " +
    s"Database root name: $dbRoot, " +
    s"Database root password: $dbPassword"
  )

  if (!serverAdmin.existsDatabase(dbName, "plocal")) {
    println(s"Creating database '$dbName'")
    serverAdmin.createDatabase(dbName, "graph", "plocal")
  }

  val db = SingleConnection.graphAsJava
  db.createVertexClass("syncLabel")
  db.createVertexClass("asyncLabel")

  println("=======================================================")
  println("Executing tests for single connection")
  println("=======================================================")

  executeTests(SingleConnection)

  println("=======================================================")
  println("Executing tests for pooled connection")
  println("=======================================================")

  executeTests(PooledConnection)


  println(s"Deleting database '$dbName'")
  serverAdmin.dropDatabase(dbName, "graph")


  def executeTests(connection: Connection) = {
    val tests = new LoadTest(connection, n)

    val connectionType = connection match {
      case SingleConnection => "single"
      case PooledConnection => "pooled"
    }

    val writeSyncTimeTaken = tests.syncWrite()
    printResult(writeSyncTimeTaken, "sync", connectionType)

    val readSyncTimeTaken = tests.syncRead()
    printResult(readSyncTimeTaken, "sync", connectionType)

    clearDatabase()

    tests.asyncWrite().onComplete {
      case Success(timeTaken) =>
        printResult(timeTaken, "async", connectionType)
      case Failure(ex) =>
        println("Async writes are failed!")
        println(ex.getMessage)
    }

    tests.asyncRead().onComplete {
      case Success(timeTaken) =>
        printResult(timeTaken, "async", connectionType)
      case Failure(ex) =>
        println("Async reads are failed!")
        println(ex.getMessage)
    }

    clearDatabase()
  }

  def clearDatabase() = {
    println("Clearing database...")
    db.executeSql("delete vertex V")
  }

  def printResult(ms: Long, mode: String, connectionType: String) = {
    println(s"Took $ms ms to execute $n operations in $mode mode for $connectionType connection type")
  }

}
