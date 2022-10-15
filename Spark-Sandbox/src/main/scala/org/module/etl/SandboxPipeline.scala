package org.module.etl

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.module.etl.utils.GenericPipeline

case class SandboxPipeline(spark: SparkSession, rawZonePath: String, curatedZonePath: String) extends GenericPipeline {

  val inputPath = s"$rawZonePath/{*}"

  val outputDatabaseName = "sandboxdb"
  val outputTableName = "sandboxtable"
  val outputDataRelativePath = s"$outputDatabaseName.db/$outputTableName/data"
  val outputCheckpointRelativePath = s"$outputDatabaseName.db/$outputTableName/checkpoint"

  val inputSchema =
    new StructType(Array(
      StructField("sandbox_field", StringType, true)
    ))

  override def execute(): Unit = {
    load(transform(extract()))
  }

  override protected def extract(): DataFrame = {
    spark
      .readStream
      .option("sep", ";")
      .option("header", "true")
      .schema(inputSchema)
      .csv(inputPath)
  }

  override protected def transform(extractedDf: DataFrame): DataFrame = {
    extractedDf
  }

  override protected def load(transformedDf: DataFrame): Unit = {

    transformedDf
      .writeStream
      .trigger(Trigger.AvailableNow())
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()

    /*
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $outputDatabaseName")

    transformedDf
      .writeStream
      .trigger(Trigger.AvailableNow())
      .outputMode("append")
      .format("delta")
      .option("path", s"$outputDataRelativePath")
      .option("checkpointLocation", s"$curatedZonePath/$outputCheckpointRelativePath")
      .toTable(s"$outputDatabaseName.$outputTableName")
      .awaitTermination()
    */
  }

}
