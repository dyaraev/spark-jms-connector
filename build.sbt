// TODO: add support for Scala 2.13.*
inThisBuild(
  List(
    organization := "io.github.dyaraev",
    scalaVersion := "2.12.20",
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
    scalacOptions += "-Ywarn-unused-import",
  )
)

lazy val assemblySettings = Seq(
  assembly / assemblyJarName := s"spark-jms-${name.value}-${version.value}.jar",
  assembly / assemblyOption ~= { _.withIncludeScala(false) },
)

val sparkVersion = "3.5.7"
val activeMqVersion = "6.1.7"
val jakartaJmsVersion = "3.1.0"
val scalaTestVersion = "3.2.19"
val scalaLoggingVersion = "3.9.6"
val deltaVersion = "3.2.1"
val jacksonVersion = "2.15.2" // should be the same as the one used in ActiveMQ (for test only)

lazy val connectorDependencies = Seq(
  "jakarta.jms" % "jakarta.jms-api" % jakartaJmsVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
)

lazy val root = (project in file("."))
  .disablePlugins(AssemblyPlugin)
  .aggregate(common, connectorV1, connectorV2, examples)

lazy val common = (project in file("common"))
  .disablePlugins(AssemblyPlugin)
  .settings(
    name := "common",
    libraryDependencies ++= connectorDependencies,
  )

lazy val connectorV1 = (project in file("connector-v1"))
  .dependsOn(common)
  .settings(
    name := "connector-v1",
    assemblySettings,
    libraryDependencies ++= connectorDependencies,
  )

lazy val connectorV2 = (project in file("connector-v2"))
  .dependsOn(common)
  .settings(
    name := "connector-v2",
    assemblySettings,
    libraryDependencies ++= connectorDependencies,
  )

lazy val examples = (project in file("examples"))
  .disablePlugins(AssemblyPlugin)
  .dependsOn(common, connectorV1, connectorV2)
  .settings(
    name := "examples",
    run / fork := true,
    run / javaOptions ++= Seq("--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED"),
    libraryDependencies ++= Seq(
      "jakarta.jms" % "jakarta.jms-api" % jakartaJmsVersion,
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "org.apache.activemq" % "activemq-broker" % activeMqVersion excludeAll (
        ExclusionRule("com.fasterxml.jackson.module", "jackson-module-scala"),
        ExclusionRule("com.fasterxml.jackson.core", "jackson-databind"),
      ),
      "org.apache.activemq" % "activemq-kahadb-store" % activeMqVersion excludeAll (
        ExclusionRule("com.fasterxml.jackson.module", "jackson-module-scala"),
        ExclusionRule("com.fasterxml.jackson.core", "jackson-databind"),
      ),
      "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
    ),
  )
