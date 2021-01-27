package com.grey.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

class OuterJoin(spark: SparkSession) {

  def outerJoin(): Unit = {

    println("\n\n SQL Outer Join Types")

    // left join
    val leftJoin: DataFrame = spark.sql("SELECT investors.uuid, investors.name, investors.type, " +
      "investors.investment_count, investors.created_at, companies.country_code, companies.short_description " +
      "FROM investors LEFT OUTER JOIN companies ON (investors.uuid = companies.uuid) " +
      "WHERE investment_count IS NOT NULL " +
      "ORDER BY investors.created_at ASC NULLS LAST")

    println("investors that have one or more investments on record: " + leftJoin.count())
    leftJoin.show(5)




  }

}
