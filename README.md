## SQL & Datasets

**Brief Investigations**: via CrunchBase firms/organisations data

<br/>

* [Sources](#sources)
* [Development Notes](#development-notes)
  * [Logging](#logging)
  * [Software](#software)
  * [Running Apache Spark Packages](#running-apache-spark-packages) 

<br/>

### Sources

The data sets used herein are adaptations of

* http://static.crunchbase.com/data_crunchbase/bulk_export_sample.tar.gz
* https://data.crunchbase.com/docs/daily-csv-export

<br/>

### Development Notes

#### Logging

* [scala-logging](https://index.scala-lang.org/lightbend/scala-logging/scala-logging/3.9.2?target=_2.11) <br/>
    ```
    <groupId>com.typesafe.scala-logging</groupId>
    <artifactId>scala-logging_2.11</artifactId>
    <version>3.9.2</version>
    
    <groupId>ch.qos.logback</groupId>
    <artifactId>logback-classic</artifactId>
    <version>1.2.3</version>
    ```
            
    ```import com.typesafe.scalalogging.Logger```

* [ScalaLogging](https://www.playframework.com/documentation/2.6.x/ScalaLogging) <br/>
    ```
    <groupId>com.typesafe.play</groupId>
    <artifactId>play_2.11</artifactId>
    <version>2.7.7</version>
    ```
    
    ```import play.api.Logger```
    
* [Log4j](https://logging.apache.org/log4j/2.x/)
  * [Scala API](https://logging.apache.org/log4j/scala/)
  * [Tutorials](https://howtodoinjava.com/log4j/)
  * [Console Appender](https://howtodoinjava.com/log4j/log4j-console-appender-example/)


<br/>

#### Software

*  Java <br/> 
    ```
    $ java -version
    
      java version "1.8.0_181"
      Java(TM) SE Runtime Environment (build 1.8.0_181-b13) <br/> 
      Java HotSpot(TM) 64-Bit Server VM (build 25.181-b13, mixed mode)
    ```

* Scala <br/> 
    ```bash
    $ scala -version
    
      Scala code runner version 2.11.12 -- Copyright 2002-2017, LAMP/EPFL
    ```

* Spark <br/> 
    ```bash
    $ spark-submit.cmd --version # **if** Operating System **is** Windows
    
      Spark version 2.4.7
    ```

<br/> 

In terms of packaging, **Maven**, instead of **Scala Build Tool** (SBT), is now being used for all projects
  
* Maven <br/>
    ```bash
    $ mvn -version
    
      Apache Maven 3.6.3 
    
    # Packaging: either
    $ mvn clean package 
    
    # or 
    $ mvn clean install
    ```

<br/>

#### Running Apache Spark Packages 

* Either <br/>
    ```sbtshell
    $ spark-submit 
      --class com.grey.FirmsApp 
      --master local[*] 
      target/firms-...-jar-with-dependencies.jar 
        https://raw.githubusercontent.com/briefings/firms/develop/arguments.yaml
    ```

* **or** <br/>

    ```bash
    $ spark-submit
      --class com.grey.FirmsApp 
      --name "firms" 
      --master spark://10.0.0.6:7077 
      --num-executors 2 
      target/firms-...-jar-with-dependencies.jar 
        https://raw.githubusercontent.com/briefings/firms/develop/arguments.yaml
    ```
    
    The latter allows for computation analysis via the Spark Web <abbr title="User Interface">UI</abbr>.

<br/>
<br/>
<br/>

<img src="https://render.githubusercontent.com/render/math?math={\frac{1}{2}}"></img>




