package part3state

import generators.shopping._
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import part2datastreams.datagenerator.DataGenerator.SingleShoppingCartEventsGenerator

object BroadcastState {
  private type Key=String

  private sealed trait ThresholdEvent
  private case class QuantityThreshold(value: Int) extends ThresholdEvent

  private sealed trait NotificationEvent
  private case class ThresholdBreach(userId: String, sku: String, quantity: Int, threshold: Int) extends NotificationEvent
  private case class InvalidEvent(reason: String) extends NotificationEvent

  val env: StreamExecutionEnvironment =
    StreamExecutionEnvironment
      .getExecutionEnvironment

  val shoppingCartEvents: DataStream[ShoppingCartEvent] =
       env
      .addSource(new SingleShoppingCartEventsGenerator(100)) // 100 miliseconds i.e 10 events per seconds

  val eventsByUser: KeyedStream[ShoppingCartEvent, String] =
      shoppingCartEvents
      .keyBy(_.userId)

  // issue a warning if quantity > threshold
  def purchaseWarnings(): Unit = {
    val threshold = 2

    val notificationsStream =
       eventsByUser
      .filter(_.isInstanceOf[AddToShoppingCartEvent])
      .filter(_.asInstanceOf[AddToShoppingCartEvent].quantity > threshold)
      .map { event =>
        event match {
          case AddToShoppingCartEvent(userId, sku, quantity, _) =>
            //Emitting the notification event
            ThresholdBreach(userId, sku, quantity, threshold)
          case _ => InvalidEvent("Non-shopping cart event received")
        }
      }

    notificationsStream.print()
    env.execute()
  }

  // ... if the threshold CHANGES over time?
  // thresholds will be BROADCAST

  private def changingThresholds(): Unit = {
    val thresholds = env.addSource(
      new SourceFunction[ThresholdEvent] {
      override def run(ctx: SourceFunction.SourceContext[ThresholdEvent]): Unit =
        List(2,0,4,5,6,3)//range of threshholds
          .foreach
          {
            newThreshold =>
          Thread.sleep(1000)
          //Emitting ThresholdEvents
          ctx.collect(QuantityThreshold(newThreshold))
        }

      override def cancel(): Unit = ()
    })

    // broadcast state is ALWAYS a map
    val broadcastStateDescriptor = new MapStateDescriptor[String, Int]("thresholds", classOf[String], classOf[Int])
    val broadcastThresholds: BroadcastStream[ThresholdEvent] = thresholds.broadcast(broadcastStateDescriptor)

    val notificationsStream =
      eventsByUser
      .connect(broadcastThresholds)
      .process(new KeyedBroadcastProcessFunction[Key, ShoppingCartEvent, ThresholdEvent,      NotificationEvent] {
        //                                       ^ key   ^ first event      ^ broadcast        ^ output
        //this descriptor for state management in  Flink
        val thresholdsDescriptor = new MapStateDescriptor[String, Int]("thresholds", classOf[String], classOf[Int])

        override def processBroadcastElement(
                                              event: ThresholdEvent,
                                              ctx: KeyedBroadcastProcessFunction[String, ShoppingCartEvent, ThresholdEvent, NotificationEvent]#Context,
                                              out: Collector[NotificationEvent]
                                            ): Unit = {
          val state = ctx.getBroadcastState(thresholdsDescriptor)
          event match {
            case QuantityThreshold(value) =>
              println(s"Threshold updated to: $value")
              state.put("current", value)
          }
        }

        override def processElement(
                                     event: ShoppingCartEvent,
                                     ctx: KeyedBroadcastProcessFunction[String, ShoppingCartEvent, ThresholdEvent, NotificationEvent]#ReadOnlyContext,
                                     out: Collector[NotificationEvent]
                                   ): Unit = {
          val threshold = ctx.getBroadcastState(thresholdsDescriptor).get("current")
          event match {
            case AddToShoppingCartEvent(userId, sku, quantity, _) if quantity > threshold =>

              out.collect(ThresholdBreach(userId, sku, quantity, threshold))
                //emmiting event
            case AddToShoppingCartEvent(userId, sku, quantity, _) =>
              out.collect(InvalidEvent(s"Below threshold: $quantity of $sku"))

            case _ =>
              out.collect(InvalidEvent("Unrecognized event type"))
          }
        }
      })

    notificationsStream.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    changingThresholds()
  }
}