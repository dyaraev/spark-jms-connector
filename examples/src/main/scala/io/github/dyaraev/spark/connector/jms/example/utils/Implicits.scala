package io.github.dyaraev.spark.connector.jms.example.utils

import scala.util.{Failure, Success, Try}

object Implicits {

  implicit class LetSyntax[T](private val value: T) extends AnyVal {

    def let[U](f: T => U): U = f(value)
  }

  implicit class TryCondition(private val condition: Boolean) extends AnyVal {

    def raiseWhen(throwable: => Throwable): Try[Unit] =
      if (condition) Failure(throwable)
      else Success(())
  }
}
