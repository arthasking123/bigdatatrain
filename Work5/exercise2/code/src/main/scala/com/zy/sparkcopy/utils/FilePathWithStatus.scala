package com.zy.sparkcopy.utils

import org.apache.hadoop.fs.Path

sealed trait FileType extends Serializable
case object File extends FileType
case object Directory extends FileType

case class FilePathWithStatus(uri: String, fileType: FileType) extends Serializable {
  def getPath: Path = new Path(uri)

  def isDirectory: Boolean = fileType == Directory

  def isFile: Boolean = fileType == File
}

object FilePathWithStatus extends Serializable{

  def apply(fileURI: String, isDirectory: Boolean, isFile: Boolean): FilePathWithStatus = {

    val fileType = if (isDirectory) Directory else if (isFile) File else throw new RuntimeException(s"File [$fileURI] is neither a directory or file")
    new FilePathWithStatus(fileURI, fileType)
  }
}