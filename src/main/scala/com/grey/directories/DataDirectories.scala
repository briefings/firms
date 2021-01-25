package com.grey.directories

import java.io.File

import scala.util.Try
import scala.util.control.Exception

class DataDirectories {

  def make(directoryObject: File): Try[Boolean] = {

    val T: Try[Boolean] = Exception.allCatch.withTry(
      if (!directoryObject.exists()) {
        directoryObject.mkdirs()
      } else {
        true
      }
    )

    if (T.isFailure) {
      sys.error(T.failed.get.getMessage)
    } else {
      T
    }

  }

}
