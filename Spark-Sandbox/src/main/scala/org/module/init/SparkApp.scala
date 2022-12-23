package org.module.init

import org.apache.spark.sql.SparkSession
import org.module.etl.zones._

object SparkApp {

  def run(spark: SparkSession, conf: Conf): Unit = conf.pipeline() match {
    case "sandbox-pipeline" =>
      bronzesilvergold.tables.sandbox_table
        .Pipeline(spark, conf).execute()
    case "sandbox-query" =>
      diamond.queries.sandbox_query
        .Query(spark, conf).execute().show(false)
    case _ =>
      throw new Exception("Pipeline is not registered in the router!")
  }

}
