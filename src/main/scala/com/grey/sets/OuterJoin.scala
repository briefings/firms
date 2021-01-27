package com.grey.sets

import org.apache.spark.sql.{Dataset, Row, SparkSession}

class OuterJoin(spark: SparkSession) {

  def outerJoin(acquisitions: Dataset[Row], companies: Dataset[Row], investors: Dataset[Row]): Unit = {

    println("\n\n Dataset Outer Join Types")

    // Import implicits for
    //    encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
    //    implicit conversions, e.g., converting a RDD to a DataFrames.
    //    access to the "$" notation.
    import spark.implicits._

    // left join
    val leftJoin = investors.select($"uuid", $"name", $"type", $"investment_count", $"created_at")
      .join(companies.select($"uuid", $"country_code", $"short_description"),
        Seq("uuid"), joinType = "left_outer")
      .where($"investment_count".isNotNull)
      .orderBy($"created_at".asc_nulls_last)

    println("investors that have one or more investments on record: " + leftJoin.count())
    leftJoin.show(5)




  }

}
