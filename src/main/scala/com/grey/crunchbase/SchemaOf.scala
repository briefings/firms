package com.grey.crunchbase


import java.nio.file.Paths

import com.grey.directories.LocalSettings
import com.grey.inspectors.InspectArguments
import com.typesafe.scalalogging.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}

import scala.util.Try
import scala.util.control.Exception

class SchemaOf(spark: SparkSession) {

  private val localSettings = new LocalSettings()

  def schemaOf(src: String, parameters: InspectArguments.Parameters): Try[StructType] = {

    // Logging
    val logger: Logger = Logger(classOf[SchemaOf])

    // The directory of schema files
    val directoryString = localSettings.resourcesDirectory + parameters.schemata.basename +
      parameters.schemata.crunchbase

    // The schema file in question
    val fileString: String = "schemaOf" + src.split("\\.")(0).capitalize + ".json"

    // Hence
    val directoryAndFileString = Paths.get(directoryString, fileString).toString
    logger.info(fileString)

    // Read-in the schema
    val fieldProperties: Try[RDD[String]] = Exception.allCatch.withTry(
      spark.sparkContext.textFile(directoryAndFileString)
    )

    // Convert schema to StructType
    if (fieldProperties.isSuccess) {
      Exception.allCatch.withTry(
        DataType.fromJson(fieldProperties.get.collect.mkString("")).asInstanceOf[StructType]
      )
    } else {
      sys.error(fieldProperties.failed.get.getMessage)
    }

  }

}
