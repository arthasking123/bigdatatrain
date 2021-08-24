package com.zy.sparkcopy

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.net.URI

object ArgsExtractor {
  def parse(args: Array[String]): Config = {

    val parser = new scopt.OptionParser[Config]("") {
      head("sparkDistCP", "1.0")

      opt[Unit]("i")
        .action((_, c) => c.copy(ignoreErrors = true))
        .text("Ignore failures")

      opt[Int]("m")
        .action((i, c) => c.copy(maxConcurrence = i))
        .text("max concurrence")

      help("help").text("prints this usage text")

      arg[String]("[source_path...] <target_path>")
        .unbounded()
        .minOccurs(2)
        .action((u, c) => c.copy(URIs = c.URIs :+ new URI(u)))
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        //        config.options.validateOptions()
        config

      case _ =>
        throw new RuntimeException("Failed to parse arguments")
    }
  }
}

case class Config(ignoreErrors: Boolean = false, maxConcurrence: Int = 1, URIs: Seq[URI] = Seq.empty){
  def sourceAndDestPaths: (Path, Path) = {
    URIs.reverse match {
      case d :: s :: _ => (new Path(s), new Path(d))
      case _ => throw new RuntimeException("Incorrect number of URIs")
    }
  }
}

