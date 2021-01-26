package com.grey.sql

import org.apache.spark.sql.SparkSession

class InnerJoin(spark: SparkSession) {

  def innerJoin(): Unit = {

    println("\n\n SQL Inner Join")

    // Simple
    spark.sql("SELECT acquisitions.acquirer_uuid, acquisitions.acquirer_name, " +
      "acquisitions.acquisition_type, acquisitions.acquired_on, acquisitions.acquiree_name, " +
      "companies.name, companies.short_description " +
      "FROM acquisitions INNER JOIN companies " +
      "ON (acquisitions.acquirer_uuid  = companies.uuid)").show(5)


  }

}
