package com.grey


import com.grey.inspectors.InspectArguments
import com.grey.sources.DataRead
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

class DataSteps(spark: SparkSession) {

  val dataRead = new DataRead(spark = spark)

  def dataSteps(parameters: InspectArguments.Parameters): Unit = {


    /**
      * Acquisitions
      */
    val (acquisitionsFrame: DataFrame, acquisitionsSet: Dataset[Row]) = dataRead.
      dataRead(src = "acquisitions.csv", database = "crunchbase", parameters = parameters)
    acquisitionsFrame.persist(StorageLevel.MEMORY_ONLY)
    acquisitionsSet.persist(StorageLevel.MEMORY_ONLY)
    acquisitionsFrame.createOrReplaceTempView(viewName = "acquisitions")


    /**
      * Companies
      */
    val (companiesFrame: DataFrame, companiesSet: Dataset[Row]) = dataRead.
      dataRead(src = "companies.csv", database = "crunchbase", parameters = parameters)
    companiesFrame.cache()
    companiesSet.cache()
    companiesFrame.createOrReplaceTempView(viewName = "companies")


    /**
      * Investors
      */
    val (investorsFrame: DataFrame, investorsSet: Dataset[Row]) = dataRead.
      dataRead(src = "investors.csv", database = "crunchbase", parameters = parameters)
    investorsFrame.cache()
    investorsSet.cache()
    investorsFrame.createOrReplaceTempView(viewName = "investors")


    // A summary of the temporary tables
    println("\n\nIn relation to SQL, the temporary tables are")
    spark.sql("SHOW TABLES").show()


    // Queries
    new com.grey.queries.InnerJoin(spark = spark).innerJoin(acquisitions = acquisitionsSet, companies = companiesSet,
      investors = investorsSet)

    new com.grey.queries.OuterJoin(spark = spark).outerJoin(acquisitions = acquisitionsSet, companies = companiesSet,
      investors = investorsSet)


  }

}
