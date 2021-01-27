package com.grey.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

class OuterJoin(spark: SparkSession) {

  def outerJoin(): Unit = {

    println("\n\n SQL Outer Join Types")

    // left join
    val leftJoin: DataFrame = spark.sql("SELECT companies.uuid, companies.name, " +
      "investors.name as investor_name, investors.type, investors.rank, investors.created_at " +
      "FROM companies LEFT OUTER JOIN investors ON (companies.uuid = investors.uuid) " +
      "ORDER BY investors.created_at ASC NULLS LAST")

    println("left join: " + leftJoin.count())
    leftJoin.show(5)




  }

}
