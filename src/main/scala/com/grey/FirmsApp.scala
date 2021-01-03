package com.grey

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object FirmsApp {

  def main(args: Array[String]): Unit = {

    // Limiting log data streams
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Spark session instance
    val spark = SparkSession.builder().appName("firms")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // Spark logs level
    spark.sparkContext.setLogLevel("ERROR")



  }

}
