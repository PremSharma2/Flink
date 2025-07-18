package part2datastreams

import generators.shopping._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.co.{CoProcessFunction, ProcessJoinFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import part2datastreams.datagenerator.DataGenerator.{CatalogEventsGenerator, SingleShoppingCartEventsGenerator}

object MultipleStreams {

  /*
    - union
    - window join
    - interval join
    - connect
   */

  // Unioning = combining the output of multiple streams into just one
  def demoUnion(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    // define two streams of the same type
    val shoppingCartEventsKafka: DataStream[ShoppingCartEvent] =
      env.addSource(new SingleShoppingCartEventsGenerator(300, sourceId = Option("kafka")))

    val shoppingCartEventsFiles: DataStream[ShoppingCartEvent] =
      env.addSource(new SingleShoppingCartEventsGenerator(1000, sourceId = Option("files")))

    val combinedShoppingCartEventStream: DataStream[ShoppingCartEvent] =
      shoppingCartEventsKafka.union(shoppingCartEventsFiles)

    combinedShoppingCartEventStream.print()
    env.execute()
  }

  // window join = elements belong to the same window + some join condition
  /*
TODO
  Both shoppingCartEvents and catalogEvents are processed by 2 subtasks each
  Internally, Flink keyBys both streams using userId
  A co-partitioning step ensures that:
  All events with the same userId end up in the same subtask
  his is required because Flink performs local joins only —
  so all matching keys must be on the same node (or subtask/thread)
   For each (key, window) combination, Flink maintains:
  Map[
  userId,
  Map[
    window,
    {
      leftBuffer: List[ShoppingCartEvent],
      rightBuffer: List[CatalogEvent]
    }
  ]
]
TODO
    Join Logic Executes on Trigger
    You’re using Processing Time windows, so the default trigger is:
    When that happens, Flink will:
   Iterate through all pairs of:
    (leftBuffer × rightBuffer) for each (userId, window)
   Emit joined results via the .apply() function
   ProcessingTimeTrigger: fires when system time > window.end

   [ Shopping Cart Stream ]       [ Catalog Stream ]
         |                           |
         |------ join (key = userId)|
                         |
                 out.collect(String) --------> [ Map → ToUpperCase ]
                         |
               (can go to another node/thread)
         O/p event is emitted here
   */
  def demoWindowJoins(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val shoppingCartEvents = env.addSource(new SingleShoppingCartEventsGenerator(1000, sourceId = Option("kafka")))
    val catalogEvents = env.addSource(new CatalogEventsGenerator(200))

    val joinedStream =
      shoppingCartEvents
      .join(catalogEvents)
      // provide a join condition
      .where(shoppingCartEvent => shoppingCartEvent.userId)
      .equalTo(catalogEvent => catalogEvent.userId)
      // provide the same window grouping
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      // do something with correlated events
      .apply {
           (shoppingCartEvent, catalogEvent) =>
            s"User ${shoppingCartEvent.userId} " +
            s"browsed at ${catalogEvent.time} and " +
            s"bought at ${shoppingCartEvent.time}"
      }

    joinedStream.print()
    env.execute()
  }

  // interval joins = correlation between events A and B if durationMin < timeA - timeB < durationMax
  // involves EVENT TIME
  // only works on KEYED STREAMS

  def demoIntervalJoins(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // we need to extract event times from both streams
    val shoppingCartEvents =
      env.addSource(new SingleShoppingCartEventsGenerator(300, sourceId = Option("kafka")))
        .assignTimestampsAndWatermarks(
          WatermarkStrategy.forBoundedOutOfOrderness(java.time.Duration.ofMillis(500))
            .withTimestampAssigner(new SerializableTimestampAssigner[ShoppingCartEvent] {
              override def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long): Long =
                element.time.toEpochMilli
            })
        )
        .keyBy(_.userId)

    val catalogEvents = env.addSource(new CatalogEventsGenerator(500))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness(java.time.Duration.ofMillis(500))
          .withTimestampAssigner(new SerializableTimestampAssigner[CatalogEvent] {
            override def extractTimestamp(element: CatalogEvent, recordTimestamp: Long): Long =
              element.time.toEpochMilli
          })
      )
      .keyBy(_.userId)

    val intervalJoinedStream =
       shoppingCartEvents
      .intervalJoin(catalogEvents)
      .between(Time.seconds(-2), Time.seconds(2))// it will Correlate the Two Events A and B if time limit or diff does not cross 2 seconds
      .lowerBoundExclusive() // interval is by default inclusive
      .upperBoundExclusive()
      .process(new ProcessJoinFunction[ShoppingCartEvent, CatalogEvent, String] {
        override def processElement(
                                     left: ShoppingCartEvent,
                                     right: CatalogEvent,
                                     ctx: ProcessJoinFunction[ShoppingCartEvent, CatalogEvent, String]#Context,
                                     out: Collector[String]
                                   ): Unit = {
          //emit the Output event to Downstream Flink
          out.collect(s"User ${left.userId} browsed Product on Offline Store  at ${right.time} and purchased From OnLine App at   ${left.time}")
        }
      })

    intervalJoinedStream.print()
    env.execute()
  }

  // connect = two streams are treated with the same "operator"
  def demoConnect(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // two separate streams
    val shoppingCartEvents =
      env.addSource(new SingleShoppingCartEventsGenerator(100))
        .assignTimestampsAndWatermarks(
          WatermarkStrategy.forBoundedOutOfOrderness(java.time.Duration.ofMillis(500))
            .withTimestampAssigner(new SerializableTimestampAssigner[ShoppingCartEvent] {
              override def extractTimestamp(element: ShoppingCartEvent, recordTimestamp: Long): Long =
                element.time.toEpochMilli
            })
        ).setParallelism(1)


    val catalogEvents =
      env.addSource(new CatalogEventsGenerator(1000))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forBoundedOutOfOrderness(java.time.Duration.ofMillis(500))
          .withTimestampAssigner(new SerializableTimestampAssigner[CatalogEvent] {
            override def extractTimestamp(element: CatalogEvent, recordTimestamp: Long): Long =
              element.time.toEpochMilli
          })
      ).setParallelism(1)


    // connect the streams
    val connectedStream: ConnectedStreams[ShoppingCartEvent, CatalogEvent] = shoppingCartEvents.connect(catalogEvents)

    // variables - will use single-threaded
    env.setParallelism(1)
    env.setMaxParallelism(1)

    val ratioStream: DataStream[Double] = connectedStream.process(
      new CoProcessFunction[ShoppingCartEvent, CatalogEvent, Double] {

        var shoppingCartEventCount = 0
        var catalogEventCount = 0

        override def processElement1(
                                      value: ShoppingCartEvent,
                                      ctx: CoProcessFunction[ShoppingCartEvent, CatalogEvent, Double]#Context,
                                      out: Collector[Double]
                                    ): Unit = {
          shoppingCartEventCount += 1
          out.collect(shoppingCartEventCount * 100.0 / (shoppingCartEventCount + catalogEventCount))
        }

        override def processElement2(
                                      value: CatalogEvent,
                                      ctx: CoProcessFunction[ShoppingCartEvent, CatalogEvent, Double]#Context,
                                      out: Collector[Double]
                                    ): Unit = {
          catalogEventCount += 1
          out.collect(shoppingCartEventCount * 100.0 / (shoppingCartEventCount + catalogEventCount))
        }
      }
    )

    ratioStream.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    demoConnect()
  }
}
