package com.grey.directories

import java.nio.file.Paths

class LocalSettings {

  // Of environment
  val projectDirectory: String = System.getProperty("user.dir")
  val sep: String = System.getProperty("file.separator")

  // Base names
  val names = List("src", "main", "resources")

  // val resourcesDirectory = s"$projectDirectory${sep}src${sep}main${sep}resources$sep"
  val resourcesDirectory: String = Paths.get(projectDirectory, names: _* ).toString + sep

}
