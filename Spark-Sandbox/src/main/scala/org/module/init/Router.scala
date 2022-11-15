package org.module.init

import org.apache.spark.sql.SparkSession
import org.module.etl.zones._

object Router {

  def executePipeline(spark: SparkSession, conf: Conf): Unit = conf.pipeline() match {
    case "sandbox-pipeline" =>
      bronzesilvergold.tables.sandbox_table
        .Pipeline(spark, conf.rawZonePath(), conf.curatedZonePath()).execute()
    case "sandbox-query" =>
      platinum.queries.sandbox_query
        .Query(spark).execute().show(false)
    case _ =>
      throw new Exception("Pipeline is not registered in the router!")
  }

}
