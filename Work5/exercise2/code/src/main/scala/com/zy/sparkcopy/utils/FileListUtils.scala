package com.zy.sparkcopy.utils

import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try


object FileListUtils extends Logging{

    def generateSourceDestSeq(fs: FileSystem, sourcePath: Path, destPath: Path, numOfThreads: Int): Seq[(Int,(FilePathWithStatus,Path))] ={
         val toProcessDirectoryList = new java.util.concurrent.LinkedBlockingDeque[Path]
         val result = new java.util.concurrent.LinkedBlockingQueue[(Int,(FilePathWithStatus,Path))]
         val threadsWorking = new ConcurrentHashMap[UUID, Boolean]()

         toProcessDirectoryList.add(sourcePath)

         class FileGetter extends Runnable {
           private val localFS = FileSystem.get(fs.getUri, fs.getConf)
           private val uuid = UUID.randomUUID()
           threadsWorking.put(uuid, true)

           override def run(): Unit = {
             while (threadsWorking.containsValue(true)) {
               Try(Option(toProcessDirectoryList.pollFirst(50, TimeUnit.MILLISECONDS))).toOption.flatten match {
                 case None =>
                   threadsWorking.put(uuid, false)
                 case Some(p) =>
                   threadsWorking.put(uuid, true)
                   try {
                       val iterator = localFS.listLocatedStatus(p)
                       while(iterator.hasNext){
                         val fileStatus: LocatedFileStatus = iterator.next()
                         if(fileStatus.isDirectory){
                           toProcessDirectoryList.addLast(fileStatus.getPath)
                         }
                         result.add((result.size(),(FilePathWithStatus(fileStatus), new Path(fileStatus.getPath.getName.replace(sourcePath.getName, destPath.getName)) )))
                       }
                   } catch {
                     case e: Exception => throw new RuntimeException(s"Directory [$p] listing error")
                   }
               }
             }
           }
         }

      val pool = Executors.newFixedThreadPool(numOfThreads)

      logInfo(s"Beginning recursive list of [$sourcePath]")
      val tasks: Seq[Future[Unit]] = List.fill(numOfThreads)(new FileGetter).map(pool.submit).map(j => Future {
        j.get()
        ()
      }(scala.concurrent.ExecutionContext.global))

      import scala.concurrent.ExecutionContext.Implicits.global
      Await.result(Future.sequence(tasks), Duration.Inf)
      pool.shutdown()

      if (!toProcessDirectoryList.isEmpty)
        throw new RuntimeException("toProcessDirectoryList is not empty")

      result.iterator().asScala.toSeq
    }
}
