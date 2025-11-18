import sbt.*

object Dependencies {

  // Connector dependencies
  private val jakartaJmsVersion = "3.1.0"
  private val scalaTestVersion = "3.2.19"
  private val sparkVersion = "3.5.7"

  // Example dependencies
  private val activeMqVersion = "6.2.0"
  //private val deltaVersion = "3.2.1"
  private val scalaLoggingVersion = "3.9.6"
  private val declineEffectVersion = "2.5.0"

  lazy val connectorDependencies = Seq(
    "jakarta.jms" % "jakarta.jms-api" % jakartaJmsVersion,
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
    "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  )

  lazy val providerActiveMq = Seq(
    "org.apache.activemq" % "activemq-broker" % activeMqVersion excludeAll (
      ExclusionRule("com.fasterxml.jackson.module", "jackson-module-scala"),
      ExclusionRule("com.fasterxml.jackson.core", "jackson-databind"),
    ),
    "org.apache.activemq" % "activemq-kahadb-store" % activeMqVersion excludeAll (
      ExclusionRule("com.fasterxml.jackson.module", "jackson-module-scala"),
      ExclusionRule("com.fasterxml.jackson.core", "jackson-databind"),
    ),
  )

  lazy val exampleDependencies = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
    "com.monovore" %% "decline-effect" % declineEffectVersion,
  )
}
