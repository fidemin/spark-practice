package com.yunhongmin

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, StreamingQueryListener}

import java.sql.Timestamp
import java.util.concurrent.Executors
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

object StreamingMain {
  case class InputRow(user:String, timestamp: java.sql.Timestamp, activity:String)
  case class UserState(user:String, var activity:String, var start:java.sql.Timestamp, var end:java.sql.Timestamp)

  def updateUserStateWithEvent(state:UserState, input:InputRow): UserState = {
    if (Option(input.timestamp).isEmpty) {
      return state
    }

    if (state.activity == input.activity) {
      if (input.timestamp.after(state.end)) {
        state.end = input.timestamp
      }
      if (input.timestamp.before(state.start)) {
        state.start = input.timestamp
      }
    } else {
      if (input.timestamp.after(state.end)) {
        state.start = input.timestamp
        state.end = input.timestamp
        state.activity = input.activity
      }
    }

    state
  }

  def updateAcrossEvents(user:String, inputs:Iterator[InputRow], oldState:GroupState[UserState]):UserState = {
    var state:UserState =
      if (oldState.exists) oldState.get
      else UserState(user, "", new Timestamp(0L), new Timestamp(0L))

    for (input <- inputs) {
      state = updateUserStateWithEvent(state, input)
      oldState.update(state)
    }
    state
  }

  def main(args: Array[String]): Unit = {
    val sparkMaster = sys.env("SPARK_MASTER")

    val sparkBuilder = SparkSession.builder().appName("com.yunhongmin.sparkpractice")
    if (sparkMaster.nonEmpty) {
      sparkBuilder.config("spark.master", sparkMaster)
    }
    sparkBuilder.config("spark.sql.warehouse.dir", "file:/Users/yunhongmin/Programming/spark-practice/spark-warehouse/")
    sparkBuilder.enableHiveSupport()
    val sparkSession = sparkBuilder.getOrCreate()

    sparkSession.sparkContext.setLogLevel("INFO")

    sparkSession.conf.set("spark.sql.shuffle.partitions", 5)
    sparkSession.conf.set("spark.sql.streaming.schemaInference", true)

    val streaming = sparkSession.readStream
      .option("maxFilesPerTrigger", 10)
      .json("/Users/yunhongmin/Programming/Spark-The-Definitive-Guide/data/activity-data/")

    val tableName = "events_per_window"

    val withEventTime = streaming.selectExpr(
      "*",
      "cast(cast(Creation_Time as double)/1000000000 as timestamp) as event_time")

    // below needed to convert DF -> dataset using as[InputRow]

    import sparkSession.implicits._

    val eventTimeQuery = withEventTime
      .selectExpr("User as user",
        "cast(Creation_Time/1000000000 as timestamp) as timestamp", "gt as activity")
      .withWatermark("timestamp", "10 minutes")
      .as[InputRow]
      .groupByKey(_.user)
      .mapGroupsWithState(GroupStateTimeout.EventTimeTimeout)(updateAcrossEvents)
      .writeStream
      .queryName("events_per_window")
      .format("memory")
      .outputMode("update")
      .start()

    sparkSession.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        println(s"Query started: $event.id")
      }

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        println(s"Query make progress: $event.id")
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        println(s"Query make progress: $event.id")
      }
    })

//    val eventTimeQuery = withEventTime
//      .withWatermark("event_time", "10 minutes")
//      .dropDuplicates("User", "event_time")
//      .groupBy(
//        window(col("event_time"), "10 minutes", "5 minutes"),
//        col("User"))
//      .count()
//      .writeStream
//      .queryName(tableName)
//      .format("memory")
//      .outputMode("update")
//      .start()

//    val activityCounts = streaming.groupBy("gt").count()

//    val activityQuery = activityCounts.writeStream.queryName("activity_counts")
////      .trigger(Trigger.Once())
//      .format("memory").outputMode("complete")
//      .start()
//
//    // single threaded execution context
    implicit val context: ExecutionContextExecutor = ExecutionContext
      .fromExecutor(Executors.newSingleThreadExecutor())

    val f = Future {
      println("Running asynchronously on another thread")
      for (i <- 1 to 1000) {
//        sparkSession.sql(s"select * from $tableName order by window.start").show(false)
        sparkSession.sql(s"select * from $tableName order by user, start").show(false)
        Thread.sleep(5000)
      }
    }

    f.onComplete { _ =>
      println("Running when the future completes")
    }

    Await.ready(f, 1000.seconds)

    eventTimeQuery.awaitTermination()
    }
}