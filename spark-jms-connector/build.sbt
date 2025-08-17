ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.19"

val sparkVersion = "3.5.3"
val activeMqVersion = "6.1.7"
val jakartaJmsVersion = "3.1.0"
val javaxAnnotationsVersion = "1.3.2"
val scalaTestVersion = "3.2.19"
val deltaVersion = "3.2.1"
val jacksonVersion = "2.15.2" // Should be the same as the one used in ActiveMQ (for test only)

lazy val dependencies = Seq(
  "jakarta.jms" % "jakarta.jms-api" % jakartaJmsVersion,
  "io.delta" %% "delta-spark" % deltaVersion % Provided,
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion % Test,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion % Test,
  "org.apache.activemq" % "activemq-broker" % activeMqVersion % Test excludeAll (ExclusionRule("com.fasterxml.jackson.module", "jackson-module-scala"), ExclusionRule("com.fasterxml.jackson.core", "jackson-databind")),
  "org.apache.activemq" % "activemq-kahadb-store" % activeMqVersion % Test excludeAll (ExclusionRule("com.fasterxml.jackson.module", "jackson-module-scala"), ExclusionRule("com.fasterxml.jackson.core", "jackson-databind")),
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
)

lazy val assemblySettings = Seq(
  assembly / assemblyJarName := s"${name.value}-${version.value}.jar",
  assembly / assemblyOption ~= { _.withIncludeScala(false) },
)

lazy val root = (project in file("."))
  .settings(
    name := "spark-jms-connector",
    libraryDependencies ++= dependencies,
    assemblySettings,
  )
