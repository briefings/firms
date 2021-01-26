package com.grey.sets

import org.apache.spark.sql.{Dataset, Row, SparkSession}

class InnerJoin(spark: SparkSession) {

  def innerJoin(acquisitions: Dataset[Row], companies: Dataset[Row], investors: Dataset[Row]): Unit = {

    println("\n\n Dataset Inner Join")

    // For encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
    // For implicit conversions, e.g., converting a RDD to a DataFrames.
    // In order to use the "$" notation.
    import spark.implicits._

    // Simple
    acquisitions.select($"acquirer_uuid", $"acquirer_name", $"acquisition_type", $"acquired_on", $"acquiree_name")
      .join(companies.select($"uuid", $"name", $"short_description"),
        acquisitions("acquirer_uuid") === companies("uuid"),
        joinType = "inner").show(5)

    acquisitions.select($"acquirer_uuid", $"acquirer_name", $"acquisition_type", $"acquired_on", $"acquiree_name")
      .join(companies.select($"uuid".as("acquirer_uuid"), $"name", $"short_description"),
        Seq("acquirer_uuid"), joinType = "inner").show(5)


  }

}
