package org.sparksandbox.globals

import org.apache.spark.sql.SparkSession
import org.sparksandbox.etl._

object Router {

  def executePipeline(spark: SparkSession, conf: Conf): Unit = conf.pipeline() match {
    case "sandbox-pipeline" =>
      SandboxPipeline(spark, conf.rawZonePath(), conf.curatedZonePath()).execute()
    case _ =>
      throw new Exception("Pipeline is not registered in the router!")
  }

}
