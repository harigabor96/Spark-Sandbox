package org.module

import org.apache.spark.sql.SparkSession
import org.module.init.{Conf, SparkApp}

object App {

  def main(args: Array[String]): Unit = {
    val conf = new Conf(Array(
      "-r", "../storage/raw/",
      "-c", "../storage/curated/",
      //"-p", "sandbox-pipeline"
      "-p", "sandbox-query"
    ))

    val spark = SparkSession
      .builder()
      .appName("Spark-Sandbox")
      .config("spark.sql.warehouse.dir", conf.curatedZonePath())
      .config("spark.custom.raw.dir", conf.rawZonePath())
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    SparkApp.run(spark, conf)
  }

}
