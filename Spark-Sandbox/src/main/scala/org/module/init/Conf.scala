package org.module.init

import org.rogach.scallop.ScallopConf

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {

  val rawZonePath = opt[String]()
  val curatedZonePath = opt[String](required = true)
  val pipeline = opt[String](required = true)
  verify()

}
