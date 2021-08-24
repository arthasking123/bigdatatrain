package com.zy.sparkcopy.utils

import org.apache.hadoop.fs.{FileStatus, Path}

sealed trait FileType extends Serializable
case object File extends FileType
case object Directory extends FileType

case class FilePathWithStatus(path: Path, fileType: FileType) extends Serializable {
  def getPath: Path = path

  def isDirectory: Boolean = fileType == Directory

  def isFile: Boolean = fileType == File
}

object FilePathWithStatus {

  def apply(fileStatus: FileStatus): FilePathWithStatus = {

    val fileType = if (fileStatus.isDirectory) Directory else if (fileStatus.isFile) File else throw new RuntimeException(s"File [$fileStatus] is neither a directory or file")

    new FilePathWithStatus(fileStatus.getPath, fileType)
  }
}