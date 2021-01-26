package com.grey.sources

import java.nio.file.Paths

import com.grey.directories.LocalSettings
import com.grey.inspectors.InspectArguments
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

import scala.util.Try
import scala.util.control.Exception

/**
  *
  * @param spark : An instance of SparkSession
  */
class Read(spark: SparkSession) {

  private val localSettings = new LocalSettings()
  private val inspection = new Inspection()

  def read(src: String, database: String, parameters: InspectArguments.Parameters): (DataFrame, Dataset[Row]) = {

    // Implicits
    // import spark.implicits._

    // Schema of data
    val schemaOf: Try[StructType] = new SchemaOf(spark = spark).
      schemaOf(src = src, database = database, parameters = parameters)
    val caseClassOf = CaseClassOf.caseClassOf(schema = schemaOf.get)

    // This function constructs a path string w.r.t. a database & file name; in this
    // context the database is the directory basename of a data set ...
    val dataPath: (String, String) => String = (databaseName: String, fileName: String) => {
      Paths.get(localSettings.resourcesDirectory + parameters.data.basename, databaseName, fileName).toString
    }

    // Data path string
    val dataPathString: String = database match {
      case "crunchbase" => dataPath(parameters.data.crunchbase, src)
      case "entities" => dataPath(parameters.data.entities, src)
      case _ => sys.error(s"""Unknown database '$database'""")
    }

    // Sections
    val data = Exception.allCatch.withTry(
      spark.read.schema(schemaOf.get)
        .format("csv")
        .option("header", value = true)
        .option("dateFormat", "yyyy-MM-dd")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("encoding", "UTF-8")
        .load(dataPathString)
    )

    if (data.isSuccess) {
      val frame: DataFrame = inspection.inspection(src = src, database = database, data = data.get)
      (frame, frame.as(caseClassOf))
    } else {
      sys.error(data.failed.get.getMessage)
    }

  }

}
