package com.grey.queries

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  *
  * @param spark: An instance of SparkSession
  */
class OuterJoin(spark: SparkSession) {

  /**
    *
    * @param acquisitions: The Dataset of acquisitions by companies
    * @param companies: The Dataset of companies
    * @param investors: The Dataset of investors
    */
  def outerJoin(acquisitions: Dataset[Row], companies: Dataset[Row], investors: Dataset[Row]): Unit = {

    println("\n\nOuter Join Types")


    /**
      * Import implicits for
      *   encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
      *   implicit conversions, e.g., converting a RDD to a DataFrames.
      *   access to the "$" notation.
      */
    import spark.implicits._


    /**
      * Outer Left Join: sql
      */
    val leftJoin: DataFrame = spark.sql("SELECT investors.uuid, investors.name, investors.type, " +
      "investors.investment_count, investors.created_at, companies.country_code, companies.short_description " +
      "FROM investors LEFT OUTER JOIN companies ON (investors.uuid = companies.uuid) " +
      "WHERE investment_count IS NOT NULL AND investment_count > 0 " +
      "ORDER BY investors.created_at ASC NULLS LAST")


    /**
      * Outer Left Join: Dataset[Row]
      */
    val leftJoinSet: Dataset[Row] = investors.select($"uuid", $"name", $"type", $"investment_count", $"created_at")
      .join(companies.select($"uuid", $"country_code", $"short_description"),
        Seq("uuid"), joinType = "left_outer")
      .where($"investment_count".isNotNull && $"investment_count" > 0)
      .orderBy($"created_at".asc_nulls_last)


    /**
      * Hence
      */
    println(s"Left Outer Join.  Investors that have one or more investments on record\nsql: " +
      s"${leftJoin.count()}, dataset: ${leftJoinSet.count()}")

    leftJoinSet.show(5)




  }

}
