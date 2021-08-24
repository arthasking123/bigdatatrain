package com.zy.sparkcopy.utils

import com.zy.sparkcopy.Config
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils

import java.io.FileNotFoundException
import scala.util.{Failure, Success, Try}

object CopyUtils extends Logging {

   def createDirectory(destFS: FileSystem, destPath: Path, config: Config): Unit = {
    if (!destFS.exists(destPath)) {
      val result = Try {
        if (destFS.exists(destPath.getParent)) {
          destFS.mkdirs(destPath)
        }
        else throw new FileNotFoundException(s"Parent folder [${destPath.getParent}] does not exist.")
      }
      result match {
        case Success(v) => v
        case Failure(e) if config.ignoreErrors =>
          logError(s"Exception whilst creating directory [${destPath.getName}]", e)
        case Failure(e) =>
          throw e
      }
    }
  }

   def copyFile(sourceFS: FileSystem, destFS: FileSystem, sourcePathWithStatus: FilePathWithStatus, destPath: Path, config: Config): Unit = {
    Try(destFS.getFileStatus(destPath)) match {
      case Failure(_: FileNotFoundException) =>
        performCopy(sourceFS, sourcePathWithStatus, destFS, destPath, ignoreErrors = config.ignoreErrors)
      case Failure(e) if config.ignoreErrors =>
        logError(s"Exception while getting destination file information [${destPath.getName}]", e)
      case Failure(e) =>
        throw e
      case Success(_) =>
        performCopy(sourceFS, sourcePathWithStatus, destFS, destPath, ignoreErrors = config.ignoreErrors)
    }
  }

  def performCopy(sourceFS: FileSystem, sourcePathWithStatus: FilePathWithStatus, destFS: FileSystem, destPath: Path,  ignoreErrors: Boolean): Unit = {

    Try {
      var in: Option[FSDataInputStream] = None
      var out: Option[FSDataOutputStream] = None
      try {
        in = Some(sourceFS.open(sourcePathWithStatus.getPath))
        if (!destFS.exists(destPath.getParent))
          throw new RuntimeException(s"Destination folder [${destPath.getParent}] does not exist")
        out = Some(destFS.create(destPath, true))
        IOUtils.copyBytes(in.get, out.get, sourceFS.getConf.getInt("io.file.buffer.size", 4096))

      } catch {
        case e: Throwable => throw e
      } finally {
        in.foreach(_.close())
        out.foreach(_.close())
      }
    }
    match {
      case Failure(e) if ignoreErrors =>
        logError(s"Exception while copying file file: [${destPath.getName}]", e)
      case Failure(e) =>
        throw e
    }
  }

}
