package com.grey.sources

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, upper}

class InspectionFunctions {

  def acquisitions(data: DataFrame): DataFrame = {

    // The aim herein is to ensure that the contents of the listed fields are upper case text
    val reset = List("type", "acquiree_country_code", "acquiree_state_code",
      "acquirer_country_code", "acquirer_state_code", "acquisition_type")

    // Mutable variable form
    var frame = data

    // Upper case
    reset.foreach { name =>
      val temporary: String = name + "_temporary"
      frame = frame.withColumn(temporary, upper(col(name))).drop(col(name))
        .withColumnRenamed(existingName = temporary, newName = name)
    }
    frame

  }


  def companies(data: DataFrame): DataFrame = {

    // The aim herein is to ensure that the contents of the listed fields are upper case text
    val reset = List("country_code", "state_code")

    // Mutable variable form
    var frame = data

    // Upper case
    reset.foreach { name =>
      val temporary: String = name + "_temporary"
      frame = frame.withColumn(temporary, upper(col(name))).drop(col(name))
        .withColumnRenamed(existingName = temporary, newName = name)
    }
    frame

  }

  def investors(data: DataFrame): DataFrame = {

    // The aim herein is to ensure that the contents of the listed fields are upper case text
    val reset = List("country_code", "state_code")

    // Mutable variable form
    var frame = data

    // Upper case
    reset.foreach { name =>
      val temporary: String = name + "_temporary"
      frame = frame.withColumn(temporary, upper(col(name))).drop(col(name))
        .withColumnRenamed(existingName = temporary, newName = name)
    }
    frame

  }

}
