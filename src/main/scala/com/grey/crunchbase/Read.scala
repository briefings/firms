package com.grey.crunchbase

import com.grey.directories.LocalSettings
import com.grey.inspectors.InspectArguments
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

/**
  *
  * @param spark : An instance of SparkSession
  */
class Read(spark: SparkSession) {

  private val localSettings = new LocalSettings()

  def read(src: String, parameters: InspectArguments.Parameters): DataFrame = {

    // Implicits

    // Schema of data
    val schemaOf: Try[StructType] = new SchemaOf(spark = spark).schemaOf(src: String, parameters = parameters)

    // Sections
    spark.read.schema(schemaOf.get)
      .format("csv")
      .option("header", value = true)
      .option("dateFormat", "yyyy-MM-dd")
      .option("encoding", "UTF-8")
      .load(localSettings.resourcesDirectory + parameters.data.crunchbase + src)

  }

}
