package com.yunhongmin

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, countDistinct, sum}

object Shop {

  def main(args: Array[String]): Unit = {
    val sparkMaster = sys.env.get("SPARK_MASTER")

    val sparkBuilder = SparkSession.builder().appName("com.yunhongmin.sparkpractice.shop")
    if (sparkMaster.nonEmpty) {
      sparkBuilder.config("spark.master", sparkMaster.get)
    }
    sparkBuilder.enableHiveSupport()
    val sparkSession = sparkBuilder.getOrCreate()

    val dataPathOptional = sys.env.get("DATA_PATH")
    if (dataPathOptional.isEmpty) {
      throw new IllegalArgumentException("DATA_PATH env variable is required")
    }

    val dataPath = dataPathOptional.get

    sparkSession.sparkContext.setLogLevel("WARN")

    val productDf = sparkSession.read.format("parquet")
      .option("inferSchema", "true").option("header", "true")
      .load(dataPath + "/products_parquet")

    val salesDf = sparkSession.read.format("parquet")
      .option("inferSchema", "true").option("header", "true")
      .load(dataPath + "/sales_parquet")

    val sellersDf = sparkSession.read.format("parquet")
      .option("inferSchema", "true").option("header", "true")
      .load(dataPath + "/sellers_parquet")

    productDf.show()
    salesDf.show()
    sellersDf.show()

    // simple count
    val numOfProducts = productDf.count()
    val numOfSales = salesDf.count()
    val numOfSellers = sellersDf.count()

    printf(s"number of products: $numOfProducts\n")
    printf(s"number of sales: $numOfSales\n")
    printf(s"number of sellers: $numOfSellers\n")

    // unique product sold at least once
    val numOfProductSoldAtLeastOnce = salesDf.select(col("product_id")).distinct().count()
    printf(s"number of product sold at least once: $numOfProductSoldAtLeastOnce\n")

    // unique product sold at least once per day
    val salesUnqiueProductCountByDateDf = salesDf.select(col("date"), col("product_id"))
      .groupBy(col("date"))
      .agg(countDistinct("product_id").alias("unique_products_per_date"))

    salesUnqiueProductCountByDateDf.cache()

    salesUnqiueProductCountByDateDf.orderBy("date").show()
    salesUnqiueProductCountByDateDf.agg(sum("unique_products_per_date")).show()

  }
}
