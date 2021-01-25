package com.grey.inspectors

import java.net.URL

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

import scala.util.Try
import scala.util.control.Exception

object InspectArguments {

  /**
    *
    * @param args: Arguments
    * @return
    */
  def inspectArguments(args: Array[String]): Parameters = {

    // Is the string args(0) a URL?
    val isURL: Try[Boolean] = new IsURL().isURL(args(0))

    // If the string is a valid URL parse & verify its parameters
    val mapper: ObjectMapper = new ObjectMapper(new YAMLFactory())

    val getParameters: Try[Parameters] = if (isURL.isSuccess){
      Exception.allCatch.withTry(
        mapper.readValue(new URL(args(0)), classOf[Parameters])
      )
    } else {
      sys.error(isURL.failed.get.getMessage)
    }

    if (getParameters.isSuccess) {
      getParameters.get
    } else {
      sys.error(getParameters.failed.get.getMessage)
    }

  }

  /**
    *
    * @param _data: The family of data parameters
    * @param _schemata: The family of the corresponding schema parameters
    */
  class Parameters(@JsonProperty("data") _data: Data,
                  @JsonProperty("schemata") _schemata: Schemata){
    require(_data != null, "The data object is not nullable"); val data: Data = _data
    require(_schemata != null, "the schemata object is not nullable"); val schemata: Schemata = _schemata
  }

  /**
    *
    * @param _basename: The location of the data files w.r.t. the resources directory
    * @param _typeOf: The data files type
    * @param _crunchbase: The location of the CrunchBase data w.r.t. the ...
    * @param _entities: The location of the entities data w.r.t. the ...
    */
  class Data(@JsonProperty("basename") _basename: String,
                @JsonProperty("typeOf") _typeOf: String,
                @JsonProperty("crunchbase") _crunchbase: String,
                @JsonProperty("entities") _entities: String){

    require(_basename != null, "Parameter basename, i.e., the data directory basename w.r.t. the " +
      "resources directory, is required.")
    val basename: String = _basename

    require(_typeOf != null, "Parameter typeOf, i.e., the extension string of the files, is required.")
    val typeOf: String = _typeOf

    require(_crunchbase != null, "The location of the CrunchBase files, w.r.t. the data directory, is required")
    val crunchbase: String = _crunchbase

    require(_entities != null, "The location of the entities files, w.r.t. the data directory, is required")
    val entities: String = _entities

  }

  /**
    *
    * @param _basename: The location of the schema files w.r.t. the resources directory
    * @param _crunchbase: The location of the CrunchBase schema files w.r.t. the ...
    * @param _entities: The location of the entities schema files w.r.t. the ...
    */
  class Schemata(@JsonProperty("basename") _basename: String,
                 @JsonProperty("crunchbase") _crunchbase: String,
                 @JsonProperty("entities") _entities: String){

    require(_basename != null, "Parameter basename, i.e., the schemata directory basename w.r.t. the " +
      "resources directory, is required.")
    val basename: String = _basename

    require(_crunchbase != null, "The location of the CrunchBase schema files, " +
      "w.r.t. the data directory, is required")
    val crunchbase: String = _crunchbase

    require(_entities != null, "The location of the entities schema files, " +
      "w.r.t. the data directory, is required")
    val entities: String = _entities
  }

}
