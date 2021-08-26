package com.zy.sparkcopy.utils

import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try


object FileListUtils extends Logging{

    def generateSourceDestSeq(fs: FileSystem, sourcePath: Path, destPath: Path, numOfThreads: Int): Seq[(Int,(FilePathWithStatus,String))] ={
         val toProcessDirectoryList = new java.util.concurrent.LinkedBlockingDeque[Path]
         val result = new java.util.concurrent.LinkedBlockingQueue[(Int,(FilePathWithStatus,String))]
         val threadsWorking = new ConcurrentHashMap[UUID, Boolean]()

         val sourceStatus = fs.getFileStatus(sourcePath)
         if(sourceStatus.isFile){
           result.add(0, (FilePathWithStatus(sourcePath.toString, isDirectory = false, isFile = true), destPath.toString))
         }
         else if(sourceStatus.isDirectory) {
           toProcessDirectoryList.add(sourcePath)
           result.add(0, (FilePathWithStatus(sourcePath.toString, isDirectory = true, isFile = false), destPath.toString))

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
                       while (iterator.hasNext) {
                         val fileStatus: LocatedFileStatus = iterator.next()
                         if (fileStatus.isDirectory) {
                           toProcessDirectoryList.addLast(new Path(p.toString + "/" + fileStatus.getPath.getName))
                         }
                         val sourceFileURI = p.toString + "/" + fileStatus.getPath.getName
                         result.add((result.size(), (FilePathWithStatus(sourceFileURI, fileStatus.isDirectory, fileStatus.isFile), (sourceFileURI.replace(sourcePath.toString, destPath.toString)))))
                       }
                     } catch {
                       case e: Exception => throw new RuntimeException(s"Directory [${p.getName}] listing error")
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
         }
      result.iterator().asScala.toSeq
    }
}
