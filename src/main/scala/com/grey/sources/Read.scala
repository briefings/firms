package com.grey.sources

import java.nio.file.Paths

import com.grey.directories.LocalSettings
import com.grey.inspectors.InspectArguments
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.control.Exception

import scala.util.Try

/**
  *
  * @param spark : An instance of SparkSession
  */
class Read(spark: SparkSession) {

  private val localSettings = new LocalSettings()

  def read(src: String, database: String, parameters: InspectArguments.Parameters): DataFrame = {

    // Implicits
    // import spark.implicits._

    // Schema of data
    val schemaOf: Try[StructType] = new SchemaOf(spark = spark).
      schemaOf(src = src, database = database, parameters = parameters)

    // This function contructs a path string w.r.t. a database & file name; in this
    // context the database is the directory basename of a data set ...
    val dataPath: (String, String) => String = (databaseName: String, fileName: String) => {
      Paths.get(localSettings.resourcesDirectory + parameters.data.basename, databaseName, fileName).toString
    }

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

    if (data.isSuccess){
      data.get
    } else {
      sys.error(data.failed.get.getMessage)
    }


  }

}
