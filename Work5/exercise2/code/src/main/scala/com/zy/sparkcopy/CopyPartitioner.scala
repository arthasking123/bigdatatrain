package com.zy.sparkcopy

import org.apache.spark.Partitioner

case class CopyPartitioner(num: Int) extends Partitioner {

  //设置分区数
  override def numPartitions: Int = num

  //分区规则
  override def getPartition(key: Any): Int = {
    if (!key.isInstanceOf[Int]) {
      0
    } else {
      key.asInstanceOf[Int] % numPartitions
    }
  }
}
