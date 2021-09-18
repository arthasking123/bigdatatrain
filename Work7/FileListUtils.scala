
package org.apache.spark.sql.util

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}

import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.util.Try

import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}

import org.apache.spark.util.ThreadUtils

case class FileStatus(relativePath: String, fileName: String, fileSize: Long)


object FileListUtils extends Logging{

  def generateSourceSeq(fs: FileSystem, sourcePath: Path, numOfThreads: Int):
  ( scala.collection.mutable.Map[String, (Long, Int)], Seq[(Int, FileStatus)]) = {
    val toProcessDirectoryList = new java.util.concurrent.LinkedBlockingDeque[Path]

    // path -> (directory size, file count)
    var directoryFileCountSize:  scala.collection.mutable.Map[String, (Long, Int)] =  scala.collection.mutable.Map()

    val result = new java.util.concurrent.LinkedBlockingQueue[(Int, FileStatus)]
    val threadsWorking = new ConcurrentHashMap[UUID, Boolean]()

    class FileGetter extends Runnable {
        private val localFS = FileSystem.get(fs.getUri, fs.getConf)
        private val uuid = UUID.randomUUID()
        threadsWorking.put(uuid, true)

        override def run(): Unit = {
          while (threadsWorking.containsValue(true)) {
            Try(Option(toProcessDirectoryList
              .pollFirst(50, TimeUnit.MILLISECONDS))).toOption.flatten match {
              case None =>
                threadsWorking.put(uuid, false)
              case Some(p) =>
                threadsWorking.put(uuid, true)
                try {
                  val iterator = localFS.listLocatedStatus(p)
                  while (iterator.hasNext) {
                    val fileStatus: LocatedFileStatus = iterator.next()
                    if (fileStatus.isDirectory) {
                      toProcessDirectoryList.addLast(
                        new Path(p.toString + "/" + fileStatus.getPath.getName))
                    }
                    else if (fileStatus.isFile) {
                      val fileSize = fileStatus.getLen
                      val relativePath = p.toString.replace(sourcePath.getName, "")
                      result.add(result.size(),
                        FileStatus(relativePath, fileStatus.getPath.getName, fileSize))
                    }

                  }
                } catch {
                  case e: Exception =>
                    throw new RuntimeException(s"Directory [${p.getName}] listing error")
                }
            }
          }
        }
      }

      val pool = Executors.newFixedThreadPool(numOfThreads)

      logInfo(s"Beginning recursive list of [$sourcePath]")
      val tasks: Seq[Future[Unit]] =
        List.fill(numOfThreads)(new FileGetter).map(pool.submit).map(j => Future {
        j.get()
        ()
      }(scala.concurrent.ExecutionContext.global))

      import scala.concurrent.ExecutionContext.Implicits.global
      ThreadUtils.awaitResult(Future.sequence(tasks), Duration.Inf)
      pool.shutdown()

      if (!toProcessDirectoryList.isEmpty) {
        throw new RuntimeException("toProcessDirectoryList is not empty")
      }

    // calc directory filecount and size
    val seq = result.iterator().asScala.toArray
    seq.foreach(value => {
      val fileStatus = value._2
      val relativePath = fileStatus.relativePath

      if(directoryFileCountSize.contains(relativePath)) {
        val oldValue = directoryFileCountSize(relativePath)
        directoryFileCountSize
        directoryFileCountSize(relativePath) =
          (oldValue._1 + fileStatus.fileSize, oldValue._2 + 1)
      }
      else {
        directoryFileCountSize += (relativePath ->  (fileStatus.fileSize, 1))
      }
    })
    (directoryFileCountSize, seq)
  }
}