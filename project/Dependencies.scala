import sbt.*

object Dependencies {

  // Scala versions
  val scala212Version: String = "2.12.21"
  val scala213Version: String = "2.13.18"

  // Default versions
  val defaultScalaVersion: String = scala213Version
  val defaultSparkVersion: String = "3.5.7"

  // Connector dependencies
  private val jakartaJmsVersion = "3.1.0"
  private val scalaTestVersion = "3.2.19"

  // Provider dependencies
  private val activeMqVersion = "6.2.0"
  private val jacksonVersion = "2.20.1"

  // Example dependencies
  private val declineEffectVersion = "2.5.0"
  private val scalaLoggingVersion = "3.9.6"

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
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
    "com.monovore" %% "decline-effect" % declineEffectVersion,
    "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion,
  )

  def sparkDependencies(sparkVersion: String, provided: Boolean = true): Seq[ModuleID] = {
    val conf = if (provided) Provided else Compile
    Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % conf,
      "org.apache.spark" %% "spark-sql" % sparkVersion % conf,
      "org.apache.spark" %% "spark-streaming" % sparkVersion % conf,
    )
  }
}
