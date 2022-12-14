package org.module.etl.zones.bronzesilvergold.tables.sandbox_table

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.module.etl.utils.GenericPipeline
import org.module.init.Conf

case class Pipeline(spark: SparkSession, conf: Conf) extends GenericPipeline {

  val inputPath = s"${conf.rawZonePath()}/{*}"
  val inputSchema =
    new StructType(Array(
      StructField("sandbox_field", StringType, true)
    ))

  val outputDatabaseName = "sandboxdb"
  val outputTableName = "sandboxtable"
  val outputDataRelativePath = s"$outputDatabaseName.db/$outputTableName/data"
  val outputCheckpointRelativePath = s"$outputDatabaseName.db/$outputTableName/checkpoint"

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
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $outputDatabaseName")

    transformedDf
      .writeStream
      .trigger(Trigger.AvailableNow())
      .outputMode("append")
      .format("delta")
      .option("path", s"$outputDataRelativePath")
      .option("checkpointLocation", s"${conf.curatedZonePath()}/$outputCheckpointRelativePath")
      .toTable(s"$outputDatabaseName.$outputTableName")
      .awaitTermination()
  }

}
