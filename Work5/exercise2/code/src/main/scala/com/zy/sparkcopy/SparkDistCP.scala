package com.zy.sparkcopy

import com.zy.sparkcopy.utils.{ConfigSerDeser, CopyUtils, FileListUtils, FilePathWithStatus, Logging}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkDistCP extends Serializable with Logging {

    def main(args:Array[String]): Unit ={
       val sparkSession = SparkSession.builder().getOrCreate()
       val config = ArgsExtractor.parse(args)
       run(sparkSession, config)
    }

    def run(sparkSession: SparkSession,  config: Config): Unit = {
      val (sourcePath, destPath) = config.sourceAndDestPaths
      val sc = sparkSession.sparkContext

      val sourceFS = sourcePath.getFileSystem(sc.hadoopConfiguration)

      //生成(序号,(源文件（目录）->目标文件（目录）)的Seq
      val directoryFileSeq = FileListUtils.generateSourceDestSeq(sourceFS, sourcePath, destPath, config.maxConcurrence)

      //打印待复制文件列表
      var stringList: String = ""
      directoryFileSeq.foreach(info => {
        stringList += "\r\n" + info._2._1.getPath.toString + " => " + info._2._2.toString
      })
      logInfo("directoryFileSeq:" + stringList)

      //根据maxConcurrence设置复制各文件所属partition
      val partitionedRDDs = partitionFiles(directoryFileSeq, sc, config.maxConcurrence)

      //序列化hadoopConfiguration
      val serHC = new ConfigSerDeser(sc.hadoopConfiguration)
      partitionedRDDs.mapPartitions { iterator =>
        val hc = serHC.get()
        iterator.map(value => {
          val source = value._2._1
          val dest = value._2._2
          val sourceFSCache = source.getPath.getFileSystem(hc)
          val destFSCache = new Path(dest).getFileSystem(hc)
          //对于各partition,根据源类型（文件或目录），调用不同的处理方法
          if (source.isDirectory) {
            CopyUtils.createDirectory(destFSCache, new Path(dest), config)
          }
          else if (source.isFile) {
            CopyUtils.copyFile(sourceFSCache, destFSCache, source, new Path(dest), config)
          }
        })
      }.collect()
      logInfo("SparkDistCP Run Finish, Please refer to the above logs\n")
    }

  def partitionFiles(seq: Seq[(Int,(FilePathWithStatus,String))], sc: SparkContext, maxConcurrence: Int): RDD[(Int,(FilePathWithStatus, String))] = {
    val partitionCount = Math.min(maxConcurrence, seq.length)
    sc.makeRDD(seq).partitionBy(CopyPartitioner(partitionCount))
  }


}
