package com.zy.sparkcopy.utils

import com.zy.sparkcopy.Config
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.io.IOUtils

import java.io.FileNotFoundException
import scala.util.{Failure, Success, Try}

object CopyUtils extends Logging {

   def createDirectory(destFS: FileSystem, destPath: Path, config: Config): Unit = {
    val relativePath = new Path(destPath.toUri.getPath)
    if (!destFS.exists(relativePath)) {
      val result = Try {
          destFS.mkdirs(relativePath)
      }
      result match {
        case Success(v) => v
        case Failure(e) if config.ignoreErrors =>
          logError(s"Exception while creating directory [${relativePath.toString}]", e)
        case Failure(e) =>
          throw e
      }
    }
  }

   def copyFile(sourceFS: FileSystem, destFS: FileSystem, sourcePathWithStatus: FilePathWithStatus, destPath: Path, config: Config): Unit = {
    Try(destFS.getFileStatus(destPath)) match {
      case Failure(_: FileNotFoundException) =>
        performCopy(sourceFS, sourcePathWithStatus, destFS, destPath, config)
      case Failure(e) if config.ignoreErrors =>
        logError(s"Exception while getting destination file information [${destPath.getName}]", e)
      case Failure(e) =>
        throw e
      case Success(_) =>
        performCopy(sourceFS, sourcePathWithStatus, destFS, destPath, config)
    }
  }

  def performCopy(sourceFS: FileSystem, sourcePathWithStatus: FilePathWithStatus, destFS: FileSystem, destPath: Path,  config: Config): Unit = {

    Try {
      var in: Option[FSDataInputStream] = None
      var out: Option[FSDataOutputStream] = None
      try {
        in = Some(sourceFS.open(sourcePathWithStatus.getPath))
        if (!destFS.exists(destPath.getParent)){
          createDirectory(destFS, destPath.getParent, config)
        }
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
      case Failure(e) if config.ignoreErrors =>
        logError(s"Exception while copying file file: [${destPath.getName}]", e)
      case Failure(e) =>
        throw e
      case Success(_) =>
        logInfo(s"Copied file: [${destPath.getName}]")
    }
  }

}
