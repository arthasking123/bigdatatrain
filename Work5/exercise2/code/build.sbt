scalaVersion := "2.11.12"

lazy val scala212 = "2.12.8"
lazy val scala211 = "2.11.12"
lazy val supportedScalaVersions = List(scala212,scala211)


val sparkVersion = "2.4.3"
val scoptVersion = "3.5.0"

lazy val compilerOptions = Seq(
  "-unchecked",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-deprecation",
  "-target:jvm-1.8",
  "-encoding",
  "utf8"
)

lazy val sparkdistcp = (project in file("."))
  .settings(
    name := "sparkDistCP",
    scalacOptions ++= compilerOptions,
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
    libraryDependencies += "com.github.scopt" %% "scopt" % scoptVersion,
  )
