import java.io.File
import java.nio.file.{Files, Path}

import com.github.tototoshi.csv.CSVWriter
import util._

import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.Await

object Benchmark extends App {

  if (args.length < 3) {
    println("Usage: sbt run <N> <fileName> <doSingle>" +
      "where N is the number of vertices you want to load orientDB with in each of the tests, " +
      "fileName is the name of file for putting result output in (no overwrite) (in csv format) " +
      "and doSingle is boolean flag which indicates whether single connection tests should be performed at all. " +
      "(They can kill OrientDB on large N values (~20k))")
    System.exit(1)
  }

  val (n, fileName, doSingle) = (args(0).toInt, args(1), args(2).toBoolean)

  println("Configurations read:\n" +
    s"Complexity count: $n, " +
    s"Output file: $fileName, " +
    s"Database url: $dbUrl, " +
    s"Database root name: $dbRoot, " +
    s"Database root password: $dbPassword, " +
    s"Database maximum partition size: $dbMaxPartitionSize, " +
    s"Database maximum pool: $dbMaxPool, "
  )

  if (!serverAdmin.existsDatabase(dbName, "plocal")) {
    println(s"Creating database '$dbName'")
    serverAdmin.createDatabase(dbName, "graph", "plocal")
  }

  val db = SingleConnection.graphAsJava
  db.createVertexClass("syncLabel")
  db.createVertexClass("asyncLabel")

  println("=======================================================")
  println("Executing tests for pooled connection")
  println("=======================================================")

  val (
    pooledSyncWrite,
    pooledSyncRead,
    pooledSyncUpdate,
    pooledAsyncWrite,
    pooledAsyncRead,
    pooledAsyncUpdate
  ) = executeTests(PooledConnection)

  val file = new File(fileName)

  if (!file.exists()) {
    println(s"Creating $fileName")

    val filePath = file.toPath
    Files.createFile(filePath)

    val header =
      "Max Partition Size," +
        "Max Pool Size," +
        "Connection Type," +
        "Number of operations," +
        "SyncWrite," +
        "SyncRead," +
        "SyncUpdate," +
        "AsyncWrite," +
        "AsyncRead," +
        "AsyncUpdate\n"

    Files.write(filePath, header.getBytes)
  }

  val csv = CSVWriter.open(fileName, append = true)

  val pooledResult = Seq(
    dbMaxPartitionSize,
    dbMaxPool,
    "pooled",
    n,
    pooledSyncWrite,
    pooledSyncRead,
    pooledSyncUpdate,
    pooledAsyncWrite,
    pooledAsyncRead,
    pooledAsyncUpdate
  )

  csv.writeRow(pooledResult)

  var resultString = s"Pooled connection:\n" +
    s"Sync: Write - $pooledSyncWrite ms, Read - $pooledSyncRead ms, Update - $pooledSyncUpdate ms\n" +
    s"Async: Write - $pooledAsyncWrite ms, Read - $pooledAsyncRead ms, Update - $pooledAsyncUpdate ms"

  if (doSingle) {
    println("=======================================================")
    println("Executing tests for single connection")
    println("=======================================================")

    val (
      singleSyncWrite,
      singleSyncRead,
      singleSyncUpdate,
      singleAsyncWrite,
      singleAsyncRead,
      singleAsyncUpdate
    ) = executeTests(SingleConnection)

    val singleResult = Seq(
      dbMaxPartitionSize,
      dbMaxPool,
      "single",
      n,
      singleSyncWrite,
      singleSyncRead,
      singleSyncUpdate,
      singleAsyncWrite,
      singleAsyncRead,
      singleAsyncUpdate
    )

    csv.writeRow(singleResult)

    resultString += s"\nSingle connection:\n" +
      s"Sync: Write - $singleSyncWrite ms, Read - $singleSyncRead ms, Update - $singleSyncUpdate ms\n" +
      s"Async: Write - $singleAsyncWrite ms, Read - $singleAsyncRead ms, Update - $singleAsyncUpdate ms"
  }

  println(s"Deleting database '$dbName'")
  serverAdmin.dropDatabase(dbName, "graph")


  println("=======================================================")
  println("End results")
  println(resultString)
  println("=======================================================")


  def executeTests(connection: Connection): (Long, Long, Long, Long, Long, Long) = {
    val tests = new LoadTest(connection, n)

    val connectionType = connection match {
      case SingleConnection => "single"
      case PooledConnection => "pooled"
    }

    val writeSyncTimeTaken = tests.syncWrite()
    printResult(writeSyncTimeTaken, "sync", "write", connectionType)

    val readSyncTimeTaken = tests.syncRead()
    printResult(readSyncTimeTaken, "sync", "read", connectionType)

    clearDatabase()

    val updateSyncTimeTaken = tests.syncUpdate()
    printResult(updateSyncTimeTaken, "sync", "update", connectionType)

    clearDatabase()

    val writeF = tests.asyncWrite()
    writeF.onComplete {
      case Success(timeTaken) =>
        printResult(timeTaken, "async", "write", connectionType)
      case Failure(ex) =>
        println("Async writes are failed!")
        println(ex.getMessage)
    }

    val readF = tests.asyncRead()
    readF.onComplete {
      case Success(timeTaken) =>
        printResult(timeTaken, "async", "read", connectionType)
      case Failure(ex) =>
        println("Async reads are failed!")
        println(ex.getMessage)
    }

    clearDatabase()

    val updateF = tests.asyncRead()
    updateF.onComplete {
      case Success(timeTaken) =>
        printResult(timeTaken, "async", "update", connectionType)
      case Failure(ex) =>
        println("Async updates are failed!")
        println(ex.getMessage)
    }

    clearDatabase()

    //They are completed at this point
    val writeAsyncTimeTaken = Await.result(writeF, Duration.Inf)
    val readAsyncTimeTaken = Await.result(readF, Duration.Inf)
    val updateAsyncTimeTaken = Await.result(updateF, Duration.Inf)

    (
      writeSyncTimeTaken,
      readSyncTimeTaken,
      updateSyncTimeTaken,
      writeAsyncTimeTaken,
      readAsyncTimeTaken,
      updateAsyncTimeTaken
    )
  }

  def clearDatabase() = {
    println("Clearing database...")
    db.executeSql("delete vertex V")
  }

  def printResult(ms: Long, mode: String, action: String, connectionType: String) = {
    println(s"Took $ms ms to execute $n $action operations in $mode mode for $connectionType connection type")
  }

}
