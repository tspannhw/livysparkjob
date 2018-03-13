package com.dataflowdeveloper.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.{KryoSerializer}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.{KryoSerializer, KryoRegistrator}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

object Links {

  //// Main Spark Program
  def main(args: Array[String]) {
    Logger.getLogger("com.dataflowdeveloper.links").setLevel(Level.INFO)

    val log = Logger.getLogger("com.dataflowdeveloper.example")
    log.info("Started Example")

    val spark = SparkSession
      .builder
      .appName("Example")
      .getOrCreate()

      try {
	val shdf = spark.read.json("hdfs://princeton0.field.hortonworks.com:8020/spark2-history")
 
	shdf.printSchema()
 
	shdf.createOrReplaceTempView("sparklogs")
 
	val stuffdf = spark.sql("SELECT * FROM sparklogs")
 
	stuffdf.count()

        println("Complete.")
      } catch {
        case e: Exception =>
          log.error("Writing files after job. Exception:" + e.getMessage);
          e.printStackTrace();
      }
  }
}
