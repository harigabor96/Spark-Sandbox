package org.sparksandbox

import org.apache.spark.sql.SparkSession
import org.sparksandbox.globals.{Conf, Router}

object App {

  def main(args: Array[String]): Unit = {
    //val conf = new Conf(args)

    val conf = new Conf(Array(
      "-m", "local",
      "-r", "../storage/raw/",
      "-c", "../storage/curated/",
      "-p", "sandbox-pipeline" //, "-i", ""
    ))

    val spark = SparkSession
      .builder()
      .appName("Spark-Sandbox")
      .config("spark.sql.warehouse.dir", conf.curatedZonePath())
      .master(conf.master())
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    Router.executePipeline(spark, conf)
  }

}
