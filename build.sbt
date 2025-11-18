// TODO: add support for Scala 2.13.*

inThisBuild(
  List(
    organization := "io.github.dyaraev",
    description := "JMS Connector for Apache Spark",
    developers := List(Developer("dyaraev", "", "", url("https://github.com/dyaraev"))),
    homepage := Some(url("https://github.com/dyaraev/spark-jms-connector")),
    versionScheme := Some("early-semver"),
    scalaVersion := "2.12.20",
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
    scalacOptions ++= Seq(
      "--deprecation",
      "--feature",
      "--unchecked",
      "-Ywarn-adapted-args",
      "-Ywarn-dead-code",
      "-Ywarn-inaccessible",
      "-Ywarn-infer-any",
      "-Ywarn-nullary-override",
      "-Ywarn-nullary-unit",
      "-Ywarn-numeric-widen",
      "-Ywarn-unused",
    ),
  )
)

lazy val assemblySettings = Seq(
  assembly / assemblyJarName := s"spark-jms-${name.value}-${version.value}.jar",
  assembly / assemblyOption ~= { _.withIncludeScala(false) },
  assembly / assemblyMergeStrategy := {
    case x if x.endsWith("module-info.class") => MergeStrategy.discard
    case x                                    => (assembly / assemblyMergeStrategy).value(x)
  },
)

lazy val root = (project in file("."))
  .disablePlugins(AssemblyPlugin)
  .aggregate(common, connectorV1, connectorV2, providerActiveMq, examples)

lazy val common = (project in file("common"))
  .disablePlugins(AssemblyPlugin)
  .settings(
    name := "common",
    libraryDependencies ++= Dependencies.connectorDependencies,
  )

lazy val connectorV1 = (project in file("connector-v1"))
  .dependsOn(common)
  .settings(
    name := "connector-v1",
    assemblySettings,
    libraryDependencies ++= Dependencies.connectorDependencies,
  )

lazy val connectorV2 = (project in file("connector-v2"))
  .dependsOn(common)
  .settings(
    name := "connector-v2",
    assemblySettings,
    libraryDependencies ++= Dependencies.connectorDependencies,
  )

lazy val providerActiveMq = (project in file("provider-activemq"))
  .dependsOn(common)
  .settings(
    name := "provider-activemq",
    assemblySettings,
    libraryDependencies ++= Dependencies.providerActiveMq,
  )

lazy val examples = (project in file("examples"))
  .disablePlugins(AssemblyPlugin)
  .dependsOn(common, connectorV1, connectorV2, providerActiveMq)
  .settings(
    name := "examples",
    run / fork := true,
    run / javaOptions ++= Seq("--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED"),
    libraryDependencies ++= Dependencies.exampleDependencies,
  )
