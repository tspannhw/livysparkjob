package com.dataflowdeveloper.example

import org.apache.spark.sql.SparkSession

class Example () {

  def run( spark: SparkSession) { 

      try {
        println("Started")

	val shdf = spark.read.json("hdfs://princeton0.field.hortonworks.com:8020/spark2-history")
 
	shdf.printSchema()
 
	shdf.createOrReplaceTempView("sparklogs")
 
	val stuffdf = spark.sql("SELECT * FROM sparklogs")
 
	stuffdf.count()

        println("Complete.")
      } catch {
        case e: Exception =>
          e.printStackTrace();
      }
  }
}
