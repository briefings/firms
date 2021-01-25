package com.grey.inspectors

import java.net.{HttpURLConnection, URL}

import scala.util.Try
import scala.util.control.Exception

class IsURL {
  
  def isURL(urlString: String): Try[Boolean] = {

    // Do not follow re-directs
    HttpURLConnection.setFollowRedirects(false)

    // Determine whether the URL exists
    val urlObject: Try[Boolean] = Exception.allCatch.withTry({

      val httpURLConnection: HttpURLConnection = new URL(urlString).openConnection().asInstanceOf[HttpURLConnection]
      httpURLConnection.setInstanceFollowRedirects(false)
      httpURLConnection.setRequestMethod("HEAD")
      httpURLConnection.setConnectTimeout(10000) // milliseconds
      httpURLConnection.setReadTimeout(10000) // milliseconds

      httpURLConnection.getResponseCode == HttpURLConnection.HTTP_OK

    })

    // Hence
    if (urlObject.isSuccess) {
      urlObject
    } else {
      sys.error(urlObject.failed.get.getMessage)
    }

  }

}
