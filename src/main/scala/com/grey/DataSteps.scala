package com.grey

import com.grey.directories.LocalSettings
import com.grey.inspectors.InspectArguments
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataSteps(spark: SparkSession) {

  val localSettings = new LocalSettings()

  def dataSteps(parameters: InspectArguments.Parameters): Unit = {

    val acquisitions: DataFrame = new com.grey.crunchbase.Read(spark = spark).
      read(src = "acquisitions.csv", parameters = parameters)

    acquisitions.show()

  }

}