package com.zy.sparkcopy

import com.zy.sparkcopy.utils.{CopyUtils, FileListUtils, FilePathWithStatus, Logging}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkDistCP extends Logging{

    def main(args:Array[String]): Unit ={
       val sparkSession = SparkSession.builder().getOrCreate()
       val config = ArgsExtractor.parse(args)
       run(sparkSession, config)
    }

    def run(sparkSession: SparkSession,  config: Config): Unit = {
       val (sourcePath, destPath) = config.sourceAndDestPaths
       val sc = sparkSession.sparkContext
       val sourceFS = sourcePath.getFileSystem(sc.hadoopConfiguration)
       val destFS = destPath.getFileSystem(sc.hadoopConfiguration)

       //生成(序号,(源文件（目录）->目标文件（目录）)的Seq
       val directoryFileSeq = FileListUtils.generateSourceDestSeq(sourceFS, sourcePath, destPath, config.maxConcurrence)

       //根据maxConcurrence设置复制各文件所属partition
       val partitionedRDDs = partitionFiles(directoryFileSeq,sc, config.maxConcurrence)

       //对于各partition,根据源类型（文件或目录），调用不同的处理方法
       partitionedRDDs.mapPartitions{ iterator =>
         iterator.map( value =>{
            val source = value._2._1
            val dest = value._2._2
            if(source.isDirectory){
               CopyUtils.createDirectory(destFS, dest, config)
            }
            else if(source.isFile){
               CopyUtils.copyFile(sourceFS, destFS, source, dest, config)
            }
         })
       }
       logInfo("SparkDistCP Run Finish, Please refer to the above logs\n")
    }

  def partitionFiles(seq: Seq[(Int,(FilePathWithStatus,Path))], sc: SparkContext, maxConcurrence: Int): RDD[(Int,(FilePathWithStatus, Path))] = {
    val partitionCount = Math.max(maxConcurrence, seq.length)
    sc.parallelize(seq).partitionBy(CopyPartitioner(partitionCount))
  }


}
