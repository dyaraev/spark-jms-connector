lazy val sparkVersion = settingKey[String]("Version of Apache Spark libraries")

inThisBuild(
  List(
    organization := "io.github.dyaraev",
    description := "JMS Connector for Apache Spark",
    developers := List(Developer("dyaraev", "", "", url("https://github.com/dyaraev"))),
    homepage := Some(url("https://github.com/dyaraev/spark-jms-connector")),
    versionScheme := Some("early-semver"),
    scalaVersion := Dependencies.scala213Version,
    sparkVersion := sys.props.getOrElse("spark.version", Dependencies.spark4Version),
  )
)

lazy val scalaCommonOptions = Seq(
  "--deprecation",
  "--feature",
  "--unchecked",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
)

lazy val scala212Options = Seq(
  "-Ywarn-adapted-args",
  "-Ywarn-inaccessible",
  "-Ywarn-infer-any",
  "-Ywarn-nullary-override",
  "-Ywarn-nullary-unit",
  "-Ywarn-unused",
)

lazy val scala213Options = Seq(
  "-Xlint:infer-any",
  "-Xlint:nullary-unit",
  "-Ywarn-unused:imports,patvars,locals,privates",
)

lazy val supportedScalaVersions = Def.setting {
  sparkVersion.value match {
    case x if x.startsWith("3.5.") => Seq(Dependencies.scala212Version, Dependencies.scala213Version)
    case x if x.startsWith("4.0.") => Seq(Dependencies.scala213Version)
    case _                         => sys.error(s"Unsupported Spark version: $version")
  }
}

// ===================================== Project settings ===================================== //

lazy val sparkVersionSettings = Seq(
  Compile / unmanagedSourceDirectories ++= selectSourceDirectory(
    (Compile / sourceDirectory).value,
    sparkVersion.value
  ),
  Test / unmanagedSourceDirectories ++= selectSourceDirectory(
    (Test / sourceDirectory).value,
    sparkVersion.value
  ),
)

lazy val commonSettings = Seq(
  semanticdbEnabled := true,
  semanticdbVersion := scalafixSemanticdb.revision,
  crossScalaVersions := supportedScalaVersions.value,
  Compile / scalacOptions ++= scalaOptionsForBinary(scalaBinaryVersion.value),
)

lazy val assemblySettings = Seq(
  assembly / assemblyJarName := {
    s"spark-jms-${name.value}_${scalaBinaryVersion.value}_spark-${sparkVersion.value}_${version.value}.jar"
  },
  assembly / assemblyMergeStrategy := {
    case x if x.endsWith("module-info.class") => MergeStrategy.discard
    case x                                    => (assembly / assemblyMergeStrategy).value(x)
  },
  assembly / assemblyOption ~= { _.withIncludeScala(false) },
)

// ========================================= Projects ========================================= //

lazy val root = (project in file("."))
  .disablePlugins(AssemblyPlugin)
  .aggregate(common, connectorV1, connectorV2, providerActiveMq, examples)
  .settings(commonSettings)
  .settings(
    name := "root",
    publish / skip := true,
  )

lazy val common = (project in file("common"))
  .disablePlugins(AssemblyPlugin)
  .settings(commonSettings)
  .settings(sparkVersionSettings)
  .settings(
    name := "common",
    libraryDependencies ++= Dependencies.connector ++
      Dependencies.sparkDependencies(sparkVersion.value),
  )

lazy val connectorV1 = (project in file("connector-v1"))
  .dependsOn(common)
  .settings(assemblySettings)
  .settings(commonSettings)
  .settings(sparkVersionSettings)
  .settings(
    name := "connector-v1",
    libraryDependencies ++= Dependencies.connector ++
      Dependencies.sparkDependencies(sparkVersion.value),
  )

lazy val connectorV2 = (project in file("connector-v2"))
  .dependsOn(common)
  .settings(assemblySettings)
  .settings(commonSettings)
  .settings(sparkVersionSettings)
  .settings(
    name := "connector-v2",
    libraryDependencies ++= Dependencies.connector ++
      Dependencies.sparkDependencies(sparkVersion.value),
  )

lazy val providerActiveMq = (project in file("provider-activemq"))
  .dependsOn(common % Provided)
  .settings(assemblySettings)
  .settings(commonSettings)
  .settings(
    name := "provider-activemq",
    libraryDependencies ++= Dependencies.providerActiveMq,
  )

lazy val examples = (project in file("examples"))
  .disablePlugins(AssemblyPlugin)
  .dependsOn(common, connectorV1, connectorV2, providerActiveMq)
  .settings(commonSettings)
  .settings(sparkVersionSettings)
  .settings(
    name := "examples",
    run / fork := true,
    run / javaOptions ++= Seq("--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED"),
    libraryDependencies ++= Dependencies.examples ++
      Dependencies.sparkDependencies(sparkVersion.value, provided = false),
  )

// ===================================== Helper functions ===================================== //

def scalaOptionsForBinary(scalaBinaryVersion: String): Seq[String] = {
  val versionSpecificScalaOptions = scalaBinaryVersion match {
    case "2.12" => scala212Options
    case "2.13" => scala213Options
  }
  scalaCommonOptions ++ versionSpecificScalaOptions
}

def selectSourceDirectory(baseDir: File, sparkVersion: String): Seq[File] = {
  sparkVersion match {
    case x if x.startsWith("3.5.") => Seq(baseDir / "scala-spark35")
    case x if x.startsWith("4.0.") => Seq(baseDir / "scala-spark40")
    case _                         => Seq.empty
  }
}
