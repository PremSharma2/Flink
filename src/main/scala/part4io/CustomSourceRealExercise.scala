package part4io
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

import scala.util.Random
object CustomSourceRealExercise {



    val env: StreamExecutionEnvironment =
      StreamExecutionEnvironment.getExecutionEnvironment

    /**
     * A more idiomatic and real-world aligned Flink Source Function
     * Example: Generating synthetic order events in an e-commerce platform
     */
    private case class OrderEvent(orderId: String, userId: String, amount: Double, timestamp: Long)

    private class ECommerceOrderEventSource(eventsPerSec: Double)
      extends RichParallelSourceFunction[OrderEvent] {

      @volatile private var isRunning = true
      private val maxSleepTime = (1000 / eventsPerSec).toLong
      private val users = List("Alice", "Bob", "Charlie", "Dave")

      override def open(parameters: Configuration): Unit =
        println(s"[${Thread.currentThread().getName}] ECommerceOrderEventSource started")

      override def run(ctx: SourceFunction.SourceContext[OrderEvent]): Unit = {
        val rand = new Random()
        while (isRunning) {
          val sleepTime = rand.nextInt(maxSleepTime.toInt)
          Thread.sleep(sleepTime)

          val orderEvent = OrderEvent(
            orderId = java.util.UUID.randomUUID().toString,
            userId = users(rand.nextInt(users.size)),
            amount = 50 + rand.nextInt(500),
            timestamp = System.currentTimeMillis()
          )
          ctx.collect(orderEvent)
        }
      }

      override def cancel(): Unit = {
        isRunning = false
      }

      override def close(): Unit =
        println(s"[${Thread.currentThread().getName}] ECommerceOrderEventSource stopped")
    }

    private def demoECommerceSource(): Unit = {
      val orderStream: DataStream[OrderEvent] =
           env
          .addSource(new ECommerceOrderEventSource(eventsPerSec = 5))
          .setParallelism(4)

      orderStream.print()
      env.execute("E-Commerce Order Stream Simulation")
    }

    def main(args: Array[String]): Unit = {
      demoECommerceSource()
    }



}
