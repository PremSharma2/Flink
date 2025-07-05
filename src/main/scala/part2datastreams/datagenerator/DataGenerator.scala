package part2datastreams.datagenerator

import generators.shopping._
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.watermark.Watermark

import java.util.UUID
import scala.annotation.tailrec

object DataGenerator {


  class EventGenerator[T](
                           sleepMillisBetweenEvents: Int,
                           generator: Long => T,
                           baseInstant: java.time.Instant,
                           extraDelayInMillisOnEveryTenEvents: Option[Long] = None
                         ) extends RichParallelSourceFunction[T] {
    @volatile private var running = true

    @tailrec
    private def run(
                     id: Long,
                     ctx: SourceFunction.SourceContext[T]
                   ): Unit =
      if (running) {
        ctx.collect(
          generator(id)
        )
        // this generator emits a watermark mimicking the same logic of
        // incrementing each element's timestamp
        ctx.emitWatermark(new Watermark(baseInstant.plusSeconds(id).toEpochMilli))
        Thread.sleep(sleepMillisBetweenEvents)
        if (id % 10 == 0) extraDelayInMillisOnEveryTenEvents.foreach(Thread.sleep)
        run(id + 1, ctx)
      }

    override def run(ctx: SourceFunction.SourceContext[T]): Unit =
      run(1, ctx)

    override def cancel(): Unit = {
      running = false
    }
  }


  class ShoppingCartEventsGenerator(
                                     sleepMillisPerEvent: Int,
                                     batchSize: Int,
                                     baseInstant: java.time.Instant = java.time.Instant.now()
                                   ) extends SourceFunction[ShoppingCartEvent] {

    import ShoppingCartEventsGenerator._

    @volatile private var running = true

    @tailrec
    private def run(
                     startId: Long,
                     ctx: SourceFunction.SourceContext[ShoppingCartEvent]
                   ): Unit =
      if (running) {
        generateRandomEvents(startId).foreach(ctx.collect)
        Thread.sleep(batchSize * sleepMillisPerEvent)
        run(startId + batchSize, ctx)
      }

    private def generateRandomEvents(id: Long): Seq[AddToShoppingCartEvent] = {
      val events = (1 to batchSize)
        .map(_ =>
          AddToShoppingCartEvent(
            getRandomUser,
            UUID.randomUUID().toString,
            getRandomQuantity,
            baseInstant.plusSeconds(id)
          )
        )

      events
    }

    override def run(
                      ctx: SourceFunction.SourceContext[ShoppingCartEvent]
                    ): Unit = run(0, ctx)

    override def cancel(): Unit = {
      running = false
    }
  }

  class SingleShoppingCartEventsGenerator(
                                           sleepMillisBetweenEvents: Int,
                                           baseInstant: java.time.Instant = java.time.Instant.now(),
                                           extraDelayInMillisOnEveryTenEvents: Option[Long] = None,
                                           sourceId: Option[String] = None,
                                           generateRemoved: Boolean = false
                                         ) extends EventGenerator[ShoppingCartEvent](
    sleepMillisBetweenEvents,
    SingleShoppingCartEventsGenerator.generateEvent(
      generateRemoved,
      () => sourceId
        .map(sId => s"${sId}_${UUID.randomUUID()}")
        .getOrElse(UUID.randomUUID().toString),
      baseInstant
    ),
    baseInstant,
    extraDelayInMillisOnEveryTenEvents
  )

  object SingleShoppingCartEventsGenerator {

    import ShoppingCartEventsGenerator._

    def generateEvent
    : (Boolean, () => String, java.time.Instant) => Long => ShoppingCartEvent =
      (generateRemoved, skuGen, baseInstant) =>
        id =>
          if (!generateRemoved || scala.util.Random.nextBoolean())
            AddToShoppingCartEvent(
              getRandomUser,
              skuGen(),
              getRandomQuantity,
              baseInstant.plusSeconds(id)
            )
          else
            RemovedFromShoppingCartEvent(
              getRandomUser,
              skuGen(),
              getRandomQuantity,
              baseInstant.plusSeconds(id)
            )
  }


  object ShoppingCartEventsGenerator {
    val users: Vector[String] = Vector("Bob", "Alice", "Sam", "Tom", "Diana")

    def getRandomUser: String = users(scala.util.Random.nextInt(users.length))

    def getRandomQuantity: Int = scala.util.Random.nextInt(10)
  }

  class CatalogEventsGenerator(
                                sleepMillisBetweenEvents: Int,
                                baseInstant: java.time.Instant = java.time.Instant.now(),
                                extraDelayInMillisOnEveryTenEvents: Option[Long] = None
                              ) extends EventGenerator[CatalogEvent](
    sleepMillisBetweenEvents,
    id =>
      ProductDetailsViewed(
        ShoppingCartEventsGenerator.getRandomUser,
        baseInstant.plusSeconds(id),
        UUID.randomUUID().toString
      ),
    baseInstant,
    extraDelayInMillisOnEveryTenEvents
  )
}
