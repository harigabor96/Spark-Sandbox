package org.module.etl.zones.platinum.queries.sandbox_query

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.module.etl.utils.HiveHelper
import org.module.init.Conf

case class Query(spark: SparkSession, conf: Conf) {

  def execute(): DataFrame = {
    HiveHelper.setupMetastore(spark, "sandboxdb", "sandboxtable")
    spark.sql("""
      SELECT *
      FROM sandboxdb.sandboxtable
    """)
  }

}
