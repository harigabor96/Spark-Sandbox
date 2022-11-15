package org.module.etl.utils

import org.apache.spark.sql.SparkSession

object HiveHelper {

  def setupMetastore(spark: SparkSession, curatedZonePath: String, databaseName: String, tableName: String): Unit = {
    spark.sql(
      s"""
          CREATE DATABASE IF NOT EXISTS $databaseName;
      """)
    spark.sql(
      s"""
          CREATE TABLE IF NOT EXISTS $databaseName.$tableName
          USING DELTA
          LOCATION '../$curatedZonePath/$databaseName.db/$tableName/data';
      """)
  }

}
