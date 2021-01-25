package com.grey.sources


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

  def schemaOf(src: String, database: String, parameters: InspectArguments.Parameters): Try[StructType] = {

    // Logging
    val logger: Logger = Logger(classOf[SchemaOf])

    // Schema Path
    val schemaPath: (String, String) => String = (databaseName: String, fileName: String) => {
      Paths.get(localSettings.resourcesDirectory + parameters.schemata.basename, 
        databaseName, fileName).toString
    }

    // Schema File
    val schemaFile = "schemaOf" + src.split("\\.")(0).capitalize + ".json"

    // Paths
    val schemaPathString: String = database match {
      case "crunchbase" => schemaPath(parameters.schemata.crunchbase, schemaFile)
      case "entities" => schemaPath(parameters.schemata.entities, schemaFile)
      case  _ => sys.error(s"""Unknown database '$database'""")
    }
    logger.info(schemaPathString)

    // Read-in the schema
    val fieldProperties: Try[RDD[String]] = Exception.allCatch.withTry(
      spark.sparkContext.textFile(schemaPathString)
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
