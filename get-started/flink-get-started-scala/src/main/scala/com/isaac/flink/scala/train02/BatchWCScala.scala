package com.isaac.flink.scala.train02

import org.apache.flink.api.scala.ExecutionEnvironment

object BatchWCScala {
  def main(args: Array[String]): Unit = {

    val input = "file:///myprojects/big-data-flink/data/hello.txt"

    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile(input)

    //import implicit conversion
    import org.apache.flink.api.scala._

    text.flatMap(_.toLowerCase.split("\t"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1).print()
  }
}
