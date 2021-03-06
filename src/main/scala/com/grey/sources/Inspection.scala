package com.grey.sources

import org.apache.spark.sql.DataFrame

class Inspection() {

  val inspectionFunctions = new InspectionFunctions()

  def inspection(src: String, database: String, data: DataFrame): DataFrame = {

    (src, database) match {
      case ("acquisitions.csv", "crunchbase") => inspectionFunctions.acquisitions(data = data)
      case ("companies.csv", "crunchbase") => inspectionFunctions.companies(data = data)
      case ("investors.csv", "crunchbase") => inspectionFunctions.investors(data = data)
      case _ => data
    }

  }

}
