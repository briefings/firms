package com.grey

import com.grey.inspectors.InspectArguments
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object FirmsApp {

  def main(args: Array[String]): Unit = {

    // Arguments
    if (args.length == 0) {
      sys.error("The YAML of parameters is required.")
    }

    // Inspect
    val inspectsArguments = InspectArguments
    val parameters: InspectArguments.Parameters = inspectsArguments.inspectArguments(args = args)

    // Limiting log data streams
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Spark session instance
    val spark = SparkSession.builder().appName("firms")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // Spark logs level
    spark.sparkContext.setLogLevel("ERROR")

    // Proceed
    new DataSteps(spark = spark).dataSteps(parameters = parameters)

  }

}
