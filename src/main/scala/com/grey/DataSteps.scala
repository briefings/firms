package com.grey


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

    // ... persistence
    acquisitionsFrame.persist(StorageLevel.MEMORY_ONLY)
    acquisitionsSet.persist(StorageLevel.MEMORY_ONLY)
    acquisitionsFrame.createOrReplaceTempView(viewName = "acquisitions")

    // Companies
    val (companiesFrame: DataFrame, companiesSet: Dataset[Row]) = dataRead.
      dataRead(src = "companies.csv", database = "crunchbase", parameters = parameters)

    // ... persistence
    companiesFrame.cache()
    companiesSet.cache()
    companiesFrame.createOrReplaceTempView(viewName = "companies")

    // Investors
    val (investorsFrame: DataFrame, investorsSet: Dataset[Row]) = dataRead.
      dataRead(src = "investors.csv", database = "crunchbase", parameters = parameters)

    // ... persistence
    investorsFrame.cache()
    investorsSet.cache()
    investorsFrame.createOrReplaceTempView(viewName = "investors")

    // Previews
    println("Acquisitions: " + acquisitionsSet.count())
    println("Companies: " + companiesSet.count(), companiesSet.distinct().count())
    println("Investors: " + investorsSet.count(), investorsSet.distinct().count())

    // Queries
    new com.grey.sql.InnerJoin(spark = spark).innerJoin()
    new com.grey.sets.InnerJoin(spark = spark).innerJoin(acquisitions = acquisitionsSet, companies = companiesSet,
      investors = investorsSet)

    new com.grey.sql.OuterJoin(spark = spark).outerJoin()
    new com.grey.sets.OuterJoin(spark = spark).outerJoin(acquisitions = acquisitionsSet, companies = companiesSet,
      investors = investorsSet)

  }

}
