import sbt.*

object Dependencies {

  val scala212Version = "2.12.20"
  val scala213Version = "2.13.18"

  // Connector dependencies
  val spark3Version = "3.5.7"
  val spark4Version = "4.0.1"
  private val jakartaJmsVersion = "3.1.0"
  private val scalaTestVersion = "3.2.19"

  // Provider dependencies
  private val activeMqVersion = "6.2.0"

  // Example dependencies
  private val declineEffectVersion = "2.5.0"
  private val scalaLoggingVersion = "3.9.6"

  def sparkDependencies(sparkVersion: String) = Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
  )

  lazy val connector = Seq(
    "jakarta.jms" % "jakarta.jms-api" % jakartaJmsVersion,
    "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  )

  lazy val providerActiveMq = Seq(
    "org.apache.activemq" % "activemq-broker" % activeMqVersion excludeAll (
      ExclusionRule("com.fasterxml.jackson.module", "jackson-module-scala"),
      ExclusionRule("com.fasterxml.jackson.core", "jackson-databind"),
      ExclusionRule("org.slf4j", "slf4j-api"),
    ),
    "org.apache.activemq" % "activemq-kahadb-store" % activeMqVersion excludeAll (
      ExclusionRule("com.fasterxml.jackson.module", "jackson-module-scala"),
      ExclusionRule("com.fasterxml.jackson.core", "jackson-databind"),
      ExclusionRule("org.slf4j", "slf4j-api"),
    ),
  )

  lazy val examples = Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
    "com.monovore" %% "decline-effect" % declineEffectVersion,
  )
}
