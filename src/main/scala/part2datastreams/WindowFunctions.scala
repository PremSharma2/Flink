package part2datastreams

import generators.gaming._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction, ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, GlobalWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

import java.time.Instant
import scala.concurrent.duration._

object WindowFunctions {

  // use-case: stream of events for a gaming session

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(2)

  implicit val serverStartTime: Instant =
    Instant.parse("2022-02-02T00:00:00.000Z")

  val events: List[ServerEvent] = List(
    bob.register(2.seconds), // player "Bob" registered 2s after the server started
    bob.online(2.seconds),
    sam.register(3.seconds),
    sam.online(4.seconds),
    rob.register(4.seconds),
    alice.register(4.seconds),
    mary.register(6.seconds),
    mary.online(6.seconds),
    carl.register(8.seconds),
    rob.online(10.seconds),
    alice.online(10.seconds),
    carl.online(10.seconds)
  )


  val eventStream: DataStream[ServerEvent] = env
    .fromCollection(events)
    .assignTimestampsAndWatermarks( // extract timestamps for events (event time) + watermarks
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500)) // once you get an event with time T, you will NOT accept further events with time < T - 500
        .withTimestampAssigner(new SerializableTimestampAssigner[ServerEvent] {
          override def extractTimestamp(element: ServerEvent, recordTimestamp: Long): Long =
            element.eventTime.toEpochMilli
        })
    )

  // how many players were registered every 3 seconds?
  // [0...3s] [3s...6s] [6s...9s]
  //creating the Grouped stream of windowed Stream with given time
  //DAG : Source → WatermarkAssigner (2 tasks) → network shuffle → WindowAll (1 task)
  private val threeSecondsTumblingWindow: AllWindowedStream[ServerEvent, TimeWindow] =
    eventStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
  /*
    |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
    |         |         | bob registered  | sam registered  | sam online        |       | mary registered |       | carl registered |     | rob online    |              |
    |         |         | bob online      |                 | rob registered    |       | mary online     |       |                 |     | alice online  |              |
    |         |         |                 |                 | alice registered  |       |                 |       |                 |     | carl online   |              |
    ^|------------ window one ----------- + -------------- window two ----------------- + ------------- window three -------------- + ----------- window four ----------|^
    |                                     |                                             |                                           |                                    |
    |            1 registrations          |               3 registrations               |              2 registration               |            0 registrations         |
    |     1643760000000 - 1643760003000   |        1643760005000 - 1643760006000        |       1643760006000 - 1643760009000       |    1643760009000 - 1643760012000   |
 */


  /*
  DAG
            ┌────────────┐         ┌────────────────────┐         ┌──────────────────┐
          │ TaskManager 1 ├──────▶ SourceFunction(T1) ├────┐    │                  │
          └────────────┘         └────────────────────┘    │    │                  │
  Source                                                        ├────▶  WindowAll (T3)   │
          ┌────────────┐         ┌────────────────────┐    │    │                  │
          │ TaskManager 2 ├──────▶ SourceFunction(T2) ├────┘    └──────────────────┘
          └────────────┘         └────────────────────┘

   */
  // count by windowAll
  class CountByWindowAll extends AllWindowFunction[ServerEvent, String, TimeWindow] {
    //                                                ^ input      ^ output  ^ window type
    override def apply(window: TimeWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val registrationEventCount = input.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [${window.getStart} - ${window.getEnd}] $registrationEventCount")
    }
  }


  def demoCountByWindow(): Unit = {
    /*
    Applies the given window function CountByWindowAll to each window of 3 sec each.
    The window function is called for each evaluation of the window
     for each key individually.
      The output of the window function is interpreted as a regular non-windowed stream.
     */
    val registrationsPerThreeSeconds: DataStream[String] = threeSecondsTumblingWindow.apply(new CountByWindowAll)
    registrationsPerThreeSeconds.print()
    env.execute()
  }

  // alternative: process window function which offers a much richer API (lower-level)
  class CountByWindowAllV2 extends ProcessAllWindowFunction[ServerEvent, String, TimeWindow] {

    override def process(context: Context, elements: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val window = context.window
      val registrationEventCount = elements.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [${window.getStart} - ${window.getEnd}] $registrationEventCount")
    }
  }

  def demoCountByWindow_v2(): Unit = {
    val registrationsPerThreeSeconds: DataStream[String] = threeSecondsTumblingWindow.process(new CountByWindowAllV2)
    registrationsPerThreeSeconds.print()
    env.execute()
  }

  // alternative 2: aggregate function
  private class CountByWindowV3 extends AggregateFunction[ServerEvent, Long, Long] {
    //                                             ^ input     ^ acc  ^ output

    // start counting from 0
    override def createAccumulator(): Long = 0L

    // every element increases accumulator by 1
    override def add(value: ServerEvent, accumulator: Long): Long =
      if (value.isInstanceOf[PlayerRegistered]) accumulator + 1
      else accumulator

    // push a final output out of the final accumulator
    //when watermark reaches the window end time
    override def getResult(accumulator: Long): Long = accumulator

    // accum1 + accum2 = a bigger accumulator
    override def merge(a: Long, b: Long): Long = a + b
  }

  private def demoCountByWindow_v3(): Unit = {
    /*
    Applies the given aggregation function to each window.
    The aggregation function is called for each Event,
     aggregating values incrementally and keeping the state to one accumulator per window.
     */
    val registrationsPerThreeSeconds: DataStream[Long] = threeSecondsTumblingWindow.aggregate(new CountByWindowV3)
    registrationsPerThreeSeconds.print()
    env.execute()
  }



  /**
   * Keyed streams and window functions
   */
  // each element will be assigned to a "mini-stream" for its own key
  private val streamByType: KeyedStream[ServerEvent, String] = eventStream.keyBy(e => e.getClass.getSimpleName)

  // for every key, we'll have a separate window allocation
  //i.e perKey ,perWindow events are grouped to Worker Node on flink Cluster
  private val threeSecondsTumblingWindowByType = streamByType.window(TumblingEventTimeWindows.of(Time.seconds(3)))

  /*
    === Registration Events Stream ===
    |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
    |         |         | bob registered  | sam registered  | rob registered    |       | mary registered |       | carl registered |     |               |              |
    |         |         |                 |                 | alice registered  |       |                 |       |                 |     |               |              |
    ^|------------ window one ----------- + -------------- window two ----------------- + ------------- window three -------------- + ----------- window four ----------|^
    |            1 registration           |               3 registrations               |              2 registrations              |            0 registrations         |
    |     1643760000000 - 1643760003000   |        1643760003000 - 1643760006000        |       1643760006000 - 1643760009000       |    1643760009000 - 1643760012000   |

    === Online Events Stream ===
    |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
    |         |         | bob online      |                 | sam online        |       | mary online     |       |                 |     | rob online    | carl online  |
    |         |         |                 |                 |                   |       |                 |       |                 |     | alice online  |              |
    ^|------------ window one ----------- + -------------- window two ----------------- + ------------- window three -------------- + ----------- window four ----------|^
    |            1 online                 |               1 online                      |              1 online                     |            3 online                |
    |     1643760000000 - 1643760003000   |        1643760005000 - 1643760006000        |       1643760006000 - 1643760009000       |    1643760009000 - 1643760012000   |
  */
  /*
  Flink maintains a separate window buffer for every time window, e.g.:
  Map[
  Window(start=00:00:00, end=00:00:03) → [events],
  Window(start=00:00:03, end=00:00:06) → [events],
  ...
  A window for time range [T_start, T_end) is eligible to fire when:
 global_watermark >= T_end
🔹 How late events are handled:
  If a late event arrives with eventTime < window_end - allowedLateness, it’s dropped silently
  If it's late but within allowedLateness, it's still accepted (if configured)
  When the global watermark passes the end of a window (say 00:00:06), Flink:
Calls apply(...) with:
key = "PlayerRegistered"
window = [00:03, 00:06)
Iterable[ServerEvent] = [list of matching events for key+window]
]
 Flink does:
Assigns events to subtasks using key hash
Buffers events in state: Map[Key, Map[Window, List[Events]]]
Advances watermark based on incoming event timestamps
When watermark >= window.end:
Triggers window
Passes matching Iterable[Event] to apply(...)
Emits output downstream
Clears window state

   */

  private class CountByWindow extends WindowFunction[ServerEvent, String, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit =
      out.collect(s"$key: $window, ${input.size}")
  }

  private def demoCountByTypeByWindow(): Unit = {
    val finalStream = threeSecondsTumblingWindowByType.apply(new CountByWindow)
    finalStream.print()
    env.execute()
  }

  // alternative: process function for windows
  private class CountByWindowV2 extends ProcessWindowFunction[ServerEvent, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[ServerEvent], out: Collector[String]): Unit =
      out.collect(s"$key: ${context.window}, ${elements.size}")
  }

  private def demoCountByTypeByWindow_v2(): Unit = {
    val finalStream = threeSecondsTumblingWindowByType.process(new CountByWindowV2)
    finalStream.print()
    env.execute()
  }

  // one task processes all the data for a particular key

  /**
   * Sliding Windows
   */


    /*
    TODO
       how many players were registered every 3 seconds, UPDATED EVERY 1s?
       Window Size = 3 seconds
      Slide Interval = 1 second
      slinding window looks like this
      [0s...3s] [1s...4s] [2s...5s] ...
      use case :
      Detect if a user adds 3 or more high-value items to their cart within any 10-minute period.
      Tumbling misses patterns that span window boundaries, because
      It doesn’t look back or overlap.
      Use Sliding Window when:
     You want rolling or continuous metrics
      You care about detecting patterns across time boundaries
      Sliding Window (10-min window sliding every 5-min):
[0:00 - 0:10) → T1, T2 → count = 2

[0:05 - 0:15) → T2, T3 → count = 2

[0:10 - 0:25) → T1, T2, T3 → this is logically what you want!
     */

  /*
  |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
  |         |         | bob registered  | sam registered  | sam online        |       | mary registered |       | carl registered |     | rob online    | carl online  |
  |         |         | bob online      |                 | rob registered    |       | mary online     |       |                 |     | alice online  |              |
  |         |         |                 |                 | alice registered  |       |                 |       |                 |     |               |              |
  ^|------------ window one ----------- +
                1 registration

            + ---------------- window two --------------- +
                                  2 registrations

                       + ------------------- window three ------------------- +
                                           4 registrations

                                         + ---------------- window four --------------- +
                                                          3 registrations

                                                          + ---------------- window five -------------- +
                                                                           3 registrations

                                                                               + ---------- window six -------- +
                                                                                           1 registration

                                                                                        + ------------ window seven ----------- +
                                                                                                    2 registrations

                                                                                                         + ------- window eight------- +
                                                                                                                  1 registration

                                                                                                                + ----------- window nine ----------- +
                                                                                                                        1 registration

                                                                                                                                   + ---------- window ten --------- +
                                                                                                                                              0 registrations
   */

  def demoSlidingAllWindows(): Unit = {
    val windowSize: Time = Time.seconds(3)
    val slidingTime: Time = Time.seconds(1)

    val slidingWindowsAll = eventStream.windowAll(SlidingEventTimeWindows.of(windowSize, slidingTime))

    // process the windowed stream with similar window functions
    val registrationCountByWindow = slidingWindowsAll.apply(new CountByWindowAll)

    // similar to the other example
    registrationCountByWindow.print()
    env.execute()
  }

  /**
   * Session windows = groups of events with NO MORE THAN a certain time gap in between them
   * A Session Window groups events based on periods of activity followed by inactivity —
   * unlike tumbling/sliding, which use fixed time boundaries.
   * It closes the window only after a period of inactivity — called the gap duration
   * It caters Gap-Based Closing scenerio
   * Because real-world user behavior is bursty.
   * People don’t do actions every 10 minutes exactly. For example:
   * USe case
   * Goal: Track each user session on an e-commerce site.
   * A session = a sequence of user events (page views, add to cart, search, etc.)
   * separated by at most 30 minutes of inactivity.
   */
  // how many registration events do we have NO MORE THAN 1s apart?
  /*
    |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
    |         |         | bob registered  | sam registered  | sam online        |       | mary registered |       | carl registered |     | rob online    |              |
    |         |         | bob online      |                 | rob registered    |       | mary online     |       |                 |     | alice online  |              |
    |         |         |                 |                 | alice registered  |       |                 |       |                 |     | carl online   |              |

    after filtering:
                        2                                                       5       6                 7       8                 9     10              11
    +---------+---------+-----------------+-----------------+-------------------+-------+-----------------+-------+-----------------+-----+---------------+--------------+
    |         |         | bob registered  | sam registered  | rob registered    |       | mary registered |       | carl registered |     |     N/A       |              |
    |         |         |                 |                 | alice registered  |       |                 |       |                 |     |               |              |
                        ^ ----------------- window 1 -------------------------- ^       ^ -- window 2 --- ^       ^ -- window 3 --- ^     ^ -- window 4 - ^
  */

  def demoSessionWindows(): Unit = {
    val groupBySessionWindows = eventStream.windowAll(EventTimeSessionWindows.withGap(Time.seconds(1)))

    // operate any kind of window function
    val countBySessionWindows = groupBySessionWindows.apply(new CountByWindowAll)

    // same things as before
    countBySessionWindows.print()
    env.execute()
  }

  /**
   * Global window
   * When we want to detect a pattern based on events rather than
   * Time window
   */
  // how many registration events do we receive in every 10 events we received?

  class CountByGlobalWindowAll extends AllWindowFunction[ServerEvent, String, GlobalWindow] {
    //                                                    ^ input      ^ output  ^ window type
    override def apply(window: GlobalWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val registrationEventCount = input.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [$window] $registrationEventCount")
    }
  }

  /*
  TODO
        GlobalWindows.create() assigns all elements to one never-ending window.
        CountTrigger.of(10) sets up logic:
      Fire the window once 10 events have arrived.
     apply(...) is invoked every 10 events, and:
    The window function gets the current 10 elements
      Processes and emits result
      Keeps the state (unless trigger purges it)
      1 → 10 events collected → trigger fires → apply() called → result emitted
     11 → 20 events collected → trigger fires → apply() called → result emitted
     21 → only 3 events → no trigger → no output yet

   */
  def demoGlobalWindow(): Unit = {
    val globalWindowEvents = eventStream
      .windowAll(GlobalWindows.create()) // unbounded window
      .trigger(CountTrigger.of[GlobalWindow](10))
      .apply(new CountByGlobalWindowAll)

    globalWindowEvents.print()
    env.execute()
  }

  /**
   * Exercise: what was the time window (continuous 2s) when we had THE MOST registration events?
   * - what kind of window functions should we use? ALL WINDOW FUNCTION
   * - what kind of windows should we use? SLIDING WINDOWS
   */
  class KeepWindowAndCountFunction extends AllWindowFunction[ServerEvent, (TimeWindow, Long), TimeWindow] {
    override def apply(window: TimeWindow, input: Iterable[ServerEvent], out: Collector[(TimeWindow, Long)]): Unit =
      out.collect((window, input.size))
  }

  def windowFunctionsExercise(): Unit = {
    val slidingWindows: DataStream[(TimeWindow, Long)] =
      eventStream
      .filter(_.isInstanceOf[PlayerRegistered])
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(2), Time.seconds(1)))
      .apply(new KeepWindowAndCountFunction)

    val localWindows: List[(TimeWindow, Long)] = slidingWindows.executeAndCollect().toList
    val bestWindow: (TimeWindow, Long) = localWindows.maxBy(_._2)
    println(s"The best window is ${bestWindow._1} with ${bestWindow._2} registration events.")
  }

  def main(args: Array[String]): Unit = {
    windowFunctionsExercise()
  }
}