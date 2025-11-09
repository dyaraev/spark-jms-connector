package io.github.dyaraev.spark.connector.jms.example

import cats.data.Validated
import cats.implicits._
import com.monovore.decline._
import io.github.dyaraev.spark.connector.jms.example.JmsReceiverJob.JmsReceiverJobConfig
import io.github.dyaraev.spark.connector.jms.example.JmsSenderJob.JmsSenderJobConfig
import io.github.dyaraev.spark.connector.jms.example.utils.ActiveMqBroker.ActiveMqAddress
import io.github.dyaraev.spark.connector.jms.example.utils._

import java.nio.file.{InvalidPathException, Path, Paths}
import java.util.Locale
import scala.concurrent.duration._
import scala.util.Try

object ExampleCommands {

  private val receiverJobCommand = Command("receiver-job", "Example of a Spark job with a JMS source") {

    (CommonOpts.opts, ReceiverOpts.opts, MessageGenOpts.opts).mapN { (commonOpts, receiverOpts, genOpts) =>
      val amqAddress = ActiveMqAddress(receiverOpts.brokerPort)
      (
        for {
          amqDataPath <- Try(commonOpts.workingDirectory.resolve("activemq-data"))
          outputPath <- Try(commonOpts.workingDirectory.resolve("output"))
          checkpointPath <- Try(commonOpts.workingDirectory.resolve("checkpoint"))
          jobConfig = JmsReceiverJobConfig(
            outputPath = outputPath,
            checkpointPath = checkpointPath,
            sourceFormat = receiverOpts.sourceFormat,
            brokerAddress = amqAddress,
            queueName = ReceiverOpts.QueueName,
            numPartitions = receiverOpts.numPartitions,
            receiveTimeout = receiverOpts.receiveTimeout,
            commitInterval = receiverOpts.commitInterval,
            processingTime = commonOpts.processingTime,
          )
          _ <- ActiveMqBroker.withActiveMqBroker(amqAddress, amqDataPath) { () =>
            MessageGenerator.withGenerator(
              amqAddress,
              ReceiverOpts.QueueName,
              genOpts.sendInterval,
              genOpts.logInterval,
            ) { () =>
              SparkUtils.withSparkSession()(spark => JmsReceiverJob(jobConfig).runQueryAndWait(spark))
            }
          }
        } yield ()
      ).get
    }
  }

  private val senderJobCommand = Command("sender-job", "Example of a Spark job with a JMS sink") {

    (CommonOpts.opts, SenderOpts.opts, FileGenOpts.opts).mapN { (commonOpts, senderOpts, genOpts) =>
      val amqAddress = ActiveMqAddress(senderOpts.brokerPort)
      (
        for {
          amqDataPath <- Try(commonOpts.workingDirectory.resolve("activemq-data"))
          inputPath <- Try(commonOpts.workingDirectory.resolve("input"))
          checkpointPath <- Try(commonOpts.workingDirectory.resolve("checkpoint"))
          jobConfig = JmsSenderJobConfig(
            inputPath = inputPath,
            checkpointPath = checkpointPath,
            sinkFormat = senderOpts.sinkFormat,
            brokerAddress = amqAddress,
            queueName = SenderOpts.QueueName,
            processingTime = commonOpts.processingTime,
          )
          _ <- ActiveMqBroker.withActiveMqBroker(amqAddress, amqDataPath) { () =>
            MessageReceiver.withMessageReceiver(amqAddress, SenderOpts.QueueName, 1000.millis, logEmpty = false) { () =>
              CsvFileGenerator.withGenerator(inputPath, genOpts.interval, genOpts.numRecords) { schema =>
                SparkUtils.withSparkSession()(spark => JmsSenderJob(schema, jobConfig).runQueryAndWait(spark))
              }
            }
          }
        } yield ()
      ).get
    }
  }

  val mainCommand: Command[Unit] = Command("ExampleApp", "Usage examples for the Spark JMS connector") {
    Opts.subcommand(receiverJobCommand).orElse(Opts.subcommand(senderJobCommand))
  }

  // Common options

  private case class CommonOpts(workingDirectory: Path, processingTime: Duration)

  private object CommonOpts {

    private val workingDirectoryOpt: Opts[Path] = Opts
      .option[String](long = "workingDirectory", help = "Working directory")
      .mapValidated { pathStr =>
        Validated
          .catchOnly[InvalidPathException](Paths.get(pathStr))
          .leftMap(_ => s"Invalid path: $pathStr")
          .toValidatedNel
      }

    private val processingTimeOpt: Opts[Duration] = Opts
      .option[Duration](long = "processingTime", help = "Job processing time (default: 10 seconds)")
      .withDefault(10.seconds)

    val opts: Opts[CommonOpts] = (workingDirectoryOpt, processingTimeOpt).mapN(CommonOpts.apply)
  }

  // Message generator options

  private case class MessageGenOpts(sendInterval: Duration, logInterval: Duration)

  private object MessageGenOpts {

    private val genSendIntervalOpt: Opts[Duration] = Opts
      .option[Duration](long = "msgGenSendInterval", help = "Interval for sending messages (default: 100 ms)")
      .withDefault(100.millis)

    private val genLogIntervalOpt: Opts[Duration] = Opts
      .option[Duration](long = "msgGenLogInterval", help = "Interval for logging (default: 5 seconds)")
      .withDefault(5.seconds)

    val opts: Opts[MessageGenOpts] = (genSendIntervalOpt, genLogIntervalOpt).mapN(MessageGenOpts.apply)
  }

  // File generator options

  private case class FileGenOpts(interval: Duration, numRecords: Int)

  private object FileGenOpts {

    private val MaxRecords: Int = 1000

    private val genIntervalOpt: Opts[Duration] = Opts
      .option[Duration](long = "genInterval", help = "Interval for generating new files (default: 3 seconds)")
      .withDefault(3.seconds)

    private val genNumRecordsOpt: Opts[Int] = Opts
      .option[Int](long = "genNumRecords", help = s"Records per files [1-$MaxRecords] (default: 20)")
      .validate(s"Invalid number of records, expected value [1-$MaxRecords]")(x => x >= 1 && x <= MaxRecords)
      .withDefault(20)

    val opts: Opts[FileGenOpts] = (genIntervalOpt, genNumRecordsOpt).mapN(FileGenOpts.apply)
  }

  // Receiver options

  private case class ReceiverOpts(
      sourceFormat: String,
      brokerPort: Int,
      numPartitions: Int,
      receiveTimeout: Option[Duration],
      commitInterval: Duration,
  )

  private object ReceiverOpts {

    val QueueName: String = "queue.receiver.messages"

    private val DefaultPort: Int = 61618

    private val sourceFormatOpt: Opts[String] = Opts
      .option[String](long = "sourceFormat", help = "Source format [ jms-v1 | jms-v2 ] (default: jms-v2)")
      .map(_.toLowerCase(Locale.ROOT))
      .validate("Invalid source format, expected one of [ jms-v1 | jms-v2 ]")(Set("jms-v1", "jms-v2").contains)
      .withDefault("jms-v2")

    private val brokerPortOpt: Opts[Int] = Opts
      .option[Int](long = "brokerPort", help = s"ActiveMQ port (default: $DefaultPort)")
      .validate("Invalid ActiveMQ port, expected positive value")(_ > 0)
      .withDefault(DefaultPort)

    private val numPartitionsOpt: Opts[Int] = Opts
      .option[Int](long = "numPartitions", help = "Number of partition for source data (default: 8)")
      .validate("Invalid number of partitions, expected a positive value")(_ > 0)
      .withDefault(8)

    private val receiveTimeoutOpt: Opts[Option[Duration]] = Opts
      .option[Duration](long = "receiveTimeout", help = "JMS receive timeout (default: 1000 milliseconds)")
      .withDefault(1000.millis)
      .map { value =>
        if (value.length == 0) None
        else Some(value)
      }

    private val commitIntervalOpt: Opts[Duration] = Opts
      .option[Duration](long = "commitInterval", help = "WAL commit interval (default: 5 second)")
      .withDefault(5.seconds)

    val opts: Opts[ReceiverOpts] =
      (sourceFormatOpt, brokerPortOpt, numPartitionsOpt, receiveTimeoutOpt, commitIntervalOpt).mapN(ReceiverOpts.apply)
  }

  // Sender options

  private case class SenderOpts(sinkFormat: String, brokerPort: Int)

  private object SenderOpts {

    val QueueName: String = "queue.sender.messages"

    private val DefaultPort: Int = 61617

    private val sinkFormatOpt: Opts[String] = Opts
      .option[String](long = "sinkFormat", help = "Sink format [ jms-v1 | jms-v2 ] (default: jms-v2)")
      .map(_.toLowerCase(Locale.ROOT))
      .validate("Invalid sink format, expected one of [ jms-v1 | jms-v2 ]")(Set("jms-v1", "jms-v2").contains)
      .withDefault("jms-v2")

    private val brokerPortOpt: Opts[Int] = Opts
      .option[Int](long = "brokerPort", help = s"ActiveMQ port (default: $DefaultPort)")
      .validate("Invalid ActiveMQ port, expected positive value")(_ > 0)
      .withDefault(DefaultPort)

    val opts: Opts[SenderOpts] = (sinkFormatOpt, brokerPortOpt).mapN(SenderOpts.apply)
  }
}
