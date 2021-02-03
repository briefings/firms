package com.grey.queries

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  *
  * @param spark: An instance of SparkSession
  */
class InnerJoin(spark: SparkSession) {

  /**
    *
    * @param acquisitions: The Dataset of acquisitions by companies
    * @param companies: The Dataset of companies
    * @param investors: The Dataset of investors
    */
  def innerJoin(acquisitions: Dataset[Row], companies: Dataset[Row], investors: Dataset[Row]): Unit = {

    println("\n\nInner Join")


    /**
      * Import implicits for
      *   encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
      *   implicit conversions, e.g., converting a RDD to a DataFrames.
      *   access to the "$" notation.
      */
    import spark.implicits._


    /**
      * Inner Join: sql
      */
    val innerJoin: DataFrame = spark.sql("SELECT distinct companies.uuid, companies.name, companies.short_description," +
      "acquisitions.acquirer_name as alt_name " +
      "FROM companies INNER JOIN acquisitions " +
      "ON (companies.uuid = acquisitions.acquirer_uuid)")


    /**
      * Inner Join: Dataset[Row]
      */
    val innerJoinSet: Dataset[Row] = companies.select($"uuid", $"name", $"short_description")
      .join(acquisitions.select($"acquirer_uuid".as("uuid"), $"acquirer_name".as("alt_name")),
        Seq("uuid"), joinType = "inner").distinct()

    /**
      * Hence
      */
    println(s"The # of distinct firms that have acquired another firm\nsql: " +
      s"${innerJoin.count()}, dataset:  ${innerJoinSet.count()} ")

    innerJoinSet.show(5)



  }

}
