package com.grey.sql

import org.apache.spark.sql.SparkSession

class InnerJoin(spark: SparkSession) {

  def innerJoin(): Unit = {

    println("\n\n SQL Inner Join")

    // Simple
    val innerJoin = spark.sql("SELECT distinct companies.uuid, companies.name, companies.short_description," +
      "acquisitions.acquirer_name as alt_name " +
      "FROM companies INNER JOIN acquisitions " +
      "ON (companies.uuid = acquisitions.acquirer_uuid)")

    println("firms that made acquisitions: " + innerJoin.count())
    innerJoin.show(5)


  }

}
