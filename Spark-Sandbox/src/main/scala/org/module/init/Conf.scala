package org.module.init

import org.rogach.scallop.ScallopConf

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {

  val master = opt[String](required = true)
  val rawZonePath = opt[String]() //Only for Bronze Modules
  val curatedZonePath = opt[String](required = true)
  val pipeline = opt[String](required = true)
  val ingestPreviousDays = opt[Int](default = None) //Only for Bronze Modules
  val terminateAfterMs = opt[Int]() //Only for Streaming Pipelines
  verify()

}