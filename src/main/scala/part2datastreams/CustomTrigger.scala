package part2datastreams

import generators.shopping._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

object CustomTrigger {


  // A real-world e-commerce use case: Fire early if high-value cart is detected, otherwise wait for window end
  class HighValueCartTrigger(thresholdQty: Int) extends Trigger[ShoppingCartEvent, TimeWindow] {

    override def onElement(
                            element: ShoppingCartEvent,
                            timestamp: Long,
                            window: TimeWindow,
                            ctx: Trigger.TriggerContext
                          ): TriggerResult = {
      // Register event-time timer for window end (guarantees final firing)
      ctx.registerEventTimeTimer(window.getEnd)

      // Early trigger if high-value cart detected
      element match {
        case add: AddToShoppingCartEvent if add.quantity >= thresholdQty =>
          TriggerResult.FIRE
        case _ =>
          TriggerResult.CONTINUE
      }
    }

    override def onEventTime(
                              time: Long,
                              window: TimeWindow,
                              ctx: Trigger.TriggerContext
                            ): TriggerResult = {
      if (time == window.getEnd) TriggerResult.FIRE_AND_PURGE else TriggerResult.CONTINUE
    }

    override def onProcessingTime(
                                   time: Long,
                                   window: TimeWindow,
                                   ctx: Trigger.TriggerContext
                                 ): TriggerResult = TriggerResult.CONTINUE

    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
      ctx.deleteEventTimeTimer(window.getEnd)
    }
  }




  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val baseTime = java.time.Instant.now()

    val cartEvents = env
      .addSource(new ShoppingCartEventsGenerator(500, 2)) // emit 2 events/sec
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness[ShoppingCartEvent](Duration.ofMillis(300))
          .withTimestampAssigner(new SerializableTimestampAssigner[ShoppingCartEvent] {
            override def extractTimestamp(e: ShoppingCartEvent, recordTimestamp: Long): Long =
              e.time.toEpochMilli
          })
      )
      .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
      .trigger(new HighValueCartTrigger(7)) // Early trigger if item with quantity >= 7 added
      .process(new CountEventsWindow)

    cartEvents.print()
    env.execute("Custom Trigger: High Value Cart Monitor")
  }


}


