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
    val leftJoin = companies.select($"uuid", $"name")
      .join(investors.select($"uuid", $"name".as("investor_name"), $"type", $"rank", $"created_at"),
        Seq("uuid"), joinType = "left_outer").orderBy($"created_at".asc_nulls_last)

    println("left join: " + leftJoin.count())
    leftJoin.show(5)




  }

}
