package generators

import org.apache.flink.streaming.api.functions.source.{
  RichParallelSourceFunction,
  SourceFunction
}

import java.util.UUID
import scala.annotation.tailrec
import org.apache.flink.streaming.api.watermark.Watermark

/**
 * A package describing data types and ADts.
 */
package object shopping {

  sealed trait ShoppingCartEvent {
    def userId: String

    def time: java.time.Instant
  }

  case class AddToShoppingCartEvent(
                                     userId: String,
                                     sku: String,
                                     quantity: Int,
                                     time: java.time.Instant
                                   ) extends ShoppingCartEvent

  case class RemovedFromShoppingCartEvent(
                                           userId: String,
                                           sku: String,
                                           quantity: Int,
                                           time: java.time.Instant
                                         ) extends ShoppingCartEvent

  sealed trait OutputEvent

  case class CorrelatedShoppingEvent(
                                      userId: String,
                                      browsedAt: java.time.Instant,
                                      purchasedAt: java.time.Instant,
                                      catalogEvent: CatalogEvent,
                                      shoppingCartEvent: ShoppingCartEvent
                                    ) extends OutputEvent {
    override def toString: String =
      s"[User: $userId] browsed at $browsedAt and bought at $purchasedAt " +
        s"-> (CatalogEvent: $catalogEvent, CartEvent: $shoppingCartEvent)"
  }


  sealed trait CatalogEvent {
    def userId: String

    def time: java.time.Instant
  }

  case class ProductDetailsViewed(
                                   userId: String,
                                   time: java.time.Instant,
                                   productId: String
                                 ) extends CatalogEvent


}

