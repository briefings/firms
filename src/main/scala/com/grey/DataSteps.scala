package com.grey

import com.grey.directories.LocalSettings
import com.grey.inspectors.InspectArguments
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

class DataSteps(spark: SparkSession) {

  val localSettings = new LocalSettings()


  def dataSteps(parameters: InspectArguments.Parameters): Unit = {

    import spark.implicits._

    // Data
    val (acquisitionsFrame: DataFrame, acquisitionsSet: Dataset[Row]) = new com.grey.sources.Read(spark = spark).
      read(src = "acquisitions.csv", database = "crunchbase", parameters = parameters)

    // Persistence
    acquisitionsFrame.persist(StorageLevel.MEMORY_ONLY)
    acquisitionsSet.persist(StorageLevel.MEMORY_ONLY)
    acquisitionsFrame.createOrReplaceTempView("acquisitions")

    // Previews
    acquisitionsFrame.select($"uuid", $"name", $"type",
      $"permalink", $"acquisition_type", $"price", $"price_currency_code").show(9)
    acquisitionsSet.select($"uuid", $"name", $"type",
      $"permalink", $"acquisition_type", $"price", $"price_currency_code").show(9)

  }

}
