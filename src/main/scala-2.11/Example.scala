package com.dataflowdeveloper.example

import org.apache.spark.sql.SparkSession

class Example () {

  def run( spark: SparkSession) { 

      try {
        println("Started")

	val shdf = spark.read.json("hdfs://princeton0.field.hortonworks.com:8020/smartPlugRaw")
 
	shdf.printSchema()
 
	shdf.createOrReplaceTempView("smartplug")
 
	val stuffdf = spark.sql("SELECT * FROM smartplug")
 
	stuffdf.count()

        println("Complete.")
      } catch {
        case e: Exception =>
          e.printStackTrace();
      }
  }
}
