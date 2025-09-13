package part4io

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import java.sql.{Connection, DriverManager, PreparedStatement}
import java.time.Instant
import scala.util.Random

// Simulated domain event
case class QuoteEvent(userId: String, product: String, quoteValue: Double, eventTime: Instant)

object FlinkPostgresSinkApp {

  def main(args: Array[String]): Unit = {
    val env =
      StreamExecutionEnvironment
        .getExecutionEnvironment
    env.setParallelism(1) // For local testing

    // Simulated quote event stream
    val quoteStream: DataStream[QuoteEvent] =
      env
        .fromCollection {
      List(
        QuoteEvent("U123", "car-insurance", 523.43, Instant.now()),
        QuoteEvent("U456", "home-loan", 220000.0, Instant.now()),
        QuoteEvent("U789", "energy-plan", 14.5, Instant.now()),
        QuoteEvent("U123", "travel-insurance", 89.99, Instant.now())
      )
    }

    // Add PostgreSQL sink
    quoteStream.addSink(new PostgresQuoteSink)

    env.execute("Flink PostgreSQL Sink Example")
  }

  private class PostgresQuoteSink extends RichSinkFunction[QuoteEvent] {
    private var connection: Connection = _
    private var statement: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
      // Load JDBC driver
      Class.forName("org.postgresql.Driver")
      connection = DriverManager.getConnection(
        "jdbc:postgresql://localhost:5432/flinkdb", // update as per your DB
        "docker", // username
        "docker" // password
      )
      connection.setAutoCommit(true)

      statement = connection.prepareStatement(
        "INSERT INTO user_quotes (user_id, product, quote_value, event_time) VALUES (?, ?, ?, ?)"
      )
    }

    override def invoke(event: QuoteEvent, context: SinkFunction.Context): Unit = {
      statement.setString(1, event.userId)
      statement.setString(2, event.product)
      statement.setDouble(3, event.quoteValue)
      statement.setTimestamp(4, java.sql.Timestamp.from(event.eventTime))
      statement.executeUpdate()
    }

    override def close(): Unit = {
      if (statement != null) statement.close()
      if (connection != null) connection.close()
    }
  }
}

