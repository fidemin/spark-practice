package com.yunhongmin

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import java.sql.DriverManager
import java.util.Properties

object Main {
  def main(args: Array[String]): Unit = {
    val sparkMaster = sys.env("SPARK_MASTER")

    val sparkBuilder = SparkSession.builder().appName("com.yunhongmin.sparkpractice")
    if (sparkMaster.nonEmpty) {
      sparkBuilder.config("spark.master", sparkMaster)
    }
    sparkBuilder.config("spark.sql.warehouse.dir", "file:/Users/yunhongmin/Programming/spark-practice/spark-warehouse/")
    sparkBuilder.enableHiveSupport()
    val spark = sparkBuilder.getOrCreate()

    spark.sparkContext.setLogLevel("INFO")


    val Df1 = spark.read.format("csv")
      .option("inferSchema", "true").option("header", "true")
      .load("/Users/yunhongmin/Programming/Spark-The-Definitive-Guide/data/flight-data/csv/*.csv")

    println("Table creation starts")
    // creates managed table from saveAsTable
    Df1.write.mode("overwrite").saveAsTable("flights_managed")

    // creates unmanaged table from saveAsTable from DF1
    Df1.write.mode("overwrite").option("path", "./data").saveAsTable("flights_unmanaged")

    // creates unmanaged table from sql
    spark.sql(
      """
        | CREATE TABLE IF NOT EXISTS flights_sql_unmanaged (
        |   DEST_COUNTRY STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
        |   USING JSON OPTIONS(path '/Users/yunhongmin/Programming/Spark-The-Definitive-Guide/data/flight-data/json/*.json')
        |""".stripMargin)

    spark.sql(
      """
        | CREATE TABLE IF NOT EXISTS flights_from_select USING parquet AS SELECT * FROM flights
        |""".stripMargin)

    println("Table creation ends")

    spark.sqlContext.sql("show tables").show()
    spark.catalog.listTables().show()

    val dfGroupBy = spark.table("flights")
      .groupBy("DEST_COUNTRY_NAME")
      .count()
    dfGroupBy.show(10)

    val dfGroupByFromSql = spark.sql(
      """
        | SELECT dest_country_name as new_name, sum(count) as flight_counts,
        | collect_set(origin_country_name) as origin_set from flights group by dest_country_name
        |""".stripMargin)
    dfGroupByFromSql.show(10)

    val dfFromSubquery = spark.sql(
      """
        | SELECT * FROM flights f1
        | where exists (select 1 from flights f2 where f1.dest_country_name = f2.origin_country_name)
        | and exists (select 1 from flights f2 where f2.dest_country_name = f1.origin_country_name)
        |""".stripMargin)

    dfFromSubquery.show()

    return





    //  CREATE TABLE flights (DEST_COUNTRY STRING, ORIGIN_COUNTRY_NAME STRING, count LONG) USING JSON OPTIONS(path '/Users/yunhongmin/Programming/Spark-The-Definitive-Guide/data/flight-data/json/*.json')
//
//    val df2 = spark.table("flights")
//    df2.show(1)

    spark.sql(
      """
        | SELECT * FROM flights1
        |""".stripMargin).limit(1).show()
    return


    return

    Df1.createOrReplaceTempView("flight_data")

    spark.sql(
      """
        | SELECT * FROM flight_data
        |""".stripMargin).limit(1).show()
    return

//    Df1.cache()
//    Df1.count()

    val start = System.nanoTime
    val Df2 = Df1.groupBy("DEST_COUNTRY_NAME").count().collect()
    val Df3 = Df1.groupBy("ORIGIN_COUNTRY_NAME").count().collect()
    val Df4 = Df1.groupBy("count").count().collect()
    val duration = (System.nanoTime - start) / 1e9d
    println("duration (s): " + duration)

    return


    val driver = "org.sqlite.JDBC"
    val path = "/Users/yunhongmin/Programming/Spark-The-Definitive-Guide/data/flight-data/jdbc/my-sqlite.db"

    val url = s"jdbc:sqlite:/${path}"
    val tablename = "flight_info"

//    val connection = DriverManager.getConnection(url)
//    println("connection closed: " + connection.isClosed())
//    connection.close()

    val dbDataFrame = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable", tablename)
      .option("driver", driver).load()

    println(dbDataFrame.count())

    val colName = "count"
    val lowerBound = 0L
    val upperBound = 1000L
    val numPartitions = 10
    val props = new Properties
    props.setProperty("driver", driver)

    val dbDf = spark.read.jdbc(url, tablename, colName, lowerBound, upperBound, numPartitions, props)
    dbDf.show(100)
    println("numOfPartitions=", dbDf.rdd.getNumPartitions)
    println("count=", dbDf.count())



    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/Users/yunhongmin/Programming/Spark-The-Definitive-Guide/data/retail-data/all/*.csv")
      .coalesce(5)

    df.show()

    df.write
      .format("csv")
      .option("mode", "OVERWRITE").option("dateFormat", "yyyy-MM-dd")
      .option("path", "/Users/yunhongmin/example1.csv")
      .partitionBy("Country")
      .save()

    df.printSchema()
    df.show(2)

    df.cache()
    df.createOrReplaceTempView("dfTable")

    df.select(variance("Quantity"), stddev("Quantity")).show()
    df.select(
      corr("Quantity", "UnitPrice"),
      covar_samp("Quantity", "UnitPrice")
    ).show(5)

    df.groupBy("InvoiceNo", "CustomerId").count().show()

    df.groupBy("InvoiceNo").agg(
      count("Quantity").alias("quantity"),
      expr("count(Quantity)")
    ).show(5)

    val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
    val windowSpec = Window
      .partitionBy("CustomerId", "date")
      .orderBy(col("Quantity").desc)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)
    val purchaseDenseRank = dense_rank().over(windowSpec)
    val purchaseRank = rank().over(windowSpec)

    df.where("CustomerId = 12347").show(10)

    dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")
      .select(
        col("CustomerId"),
        col("date"),
        col("Quantity"),
        purchaseRank.alias("quantityRank"),
        purchaseDenseRank.alias("quantityDenseRank"),
        maxPurchaseQuantity.alias("maxPurchaseQuantity")
      ).show(100)

    val dfNoNull = dfWithDate.na.drop()
    dfNoNull.groupBy(col("CustomerId"), col("stockCode"))
      .agg(sum(col("Quantity")).alias("quantity"))
      .orderBy(col("CustomerId").desc, col("stockCode").desc)
      .show()

    val rolledUpDf = dfNoNull.rollup("date", "Country")
      .agg(sum("Quantity").alias("total_quantity"))
      .selectExpr("date", "Country", "total_quantity")
      .orderBy("date")
    rolledUpDf.show()

    dfNoNull.cube("date", "Country")
      .agg(grouping_id(), sum("Quantity").alias("total_quantity"))
      .orderBy(col("grouping_id()").desc, col("date")).show()

//    val pivoted = dfWithDate.groupBy("date").pivot("Country").sum()

    dfNoNull.write.format("csv")
      .option("mode", "OVERWRITE").option("dateFormat", "yyyy-MM-dd")
      .option("path", "/Users/yunhongmin/example.csv").save()

  }
}