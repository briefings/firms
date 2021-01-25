package com.grey.directories

class LocalSettings {

  // Of environment
  val projectDirectory: String = System.getProperty("user.dir")
  val sep: String = System.getProperty("file.separator")

  // Resources
  val resourcesDirectory = s"$projectDirectory${sep}src${sep}main${sep}resources$sep"

}
