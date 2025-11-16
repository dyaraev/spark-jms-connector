package io.github.dyaraev.spark.connector.jms.example

import com.monovore.decline._

// Your IDE may require the following JVM option for running this code:
// --add-opens java.base/sun.nio.ch=ALL-UNNAMED
object ExampleApp extends CommandApp(ExampleCommands.mainCommand)
