package com.grey.sets

import org.apache.spark.sql.{Dataset, Row, SparkSession}

class InnerJoin(spark: SparkSession) {

  def innerJoin(acquisitions: Dataset[Row], companies: Dataset[Row], investors: Dataset[Row]): Unit = {

    println("\n\n Dataset Inner Join")

    // Import implicits for
    //    encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
    //    implicit conversions, e.g., converting a RDD to a DataFrames.
    //    access to the "$" notation.
    import spark.implicits._

    // Simple
    val innerJoin = companies.select($"uuid", $"name", $"short_description")
      .join(acquisitions.select($"acquirer_uuid".as("uuid"), $"acquirer_name".as("alt_name")),
        Seq("uuid"), joinType = "inner").distinct()

    println("firms that made acquisitions: " + innerJoin.count())
    innerJoin.show(5)

  }

}
