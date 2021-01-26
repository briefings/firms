package com.grey

import com.grey.directories.LocalSettings
import com.grey.inspectors.InspectArguments
import com.grey.sources.DataRead
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

class DataSteps(spark: SparkSession) {

  val dataRead = new DataRead(spark = spark)

  def dataSteps(parameters: InspectArguments.Parameters): Unit = {

    import spark.implicits._

    // Acquisitions
    val (acquisitionsFrame: DataFrame, acquisitionsSet: Dataset[Row]) = dataRead.
      dataRead(src = "acquisitions.csv", database = "crunchbase", parameters = parameters)

    // ... Persistence
    acquisitionsFrame.persist(StorageLevel.MEMORY_ONLY)
    acquisitionsSet.persist(StorageLevel.MEMORY_ONLY)
    acquisitionsFrame.createOrReplaceTempView("acquisitions")

    // Companies
    val (companiesFrame: DataFrame, companiesSet: Dataset[Row]) = dataRead
      .dataRead(src = "companies.csv", database = "crunchbase", parameters = parameters)

    // ... Persistence
    companiesFrame.cache()
    companiesSet.cache()
    companiesFrame.createOrReplaceTempView("companies")

    // Previews
    acquisitionsSet.select($"acquiree_uuid", $"acquiree_name", $"acquirer_uuid", $"acquirer_name",
      $"acquirer_country_code", $"acquisition_type", $"price", $"price_currency_code").show(5)
    companiesSet.select($"uuid", $"name", $"short_description", $"country_code").show(5)

  }

}
