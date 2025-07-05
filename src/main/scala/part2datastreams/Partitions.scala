package part2datastreams

import generators.shopping._
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._
import part2datastreams.datagenerator.DataGenerator.SingleShoppingCartEventsGenerator

object Partitions {

  // splitting = partitioning

  def demoPartitioner(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val shoppingCartEvents: DataStream[ShoppingCartEvent] =
      env.addSource(new SingleShoppingCartEventsGenerator(100)) // ~10 events/s

    // partitioner = logic to split the data
    val partitioner = new Partitioner[String] {
      override def partition(key: String, numPartitions: Int): Int = { // invoked on every event
        // hash code % number of partitions ~ even distribution
        println(s"Number of max partitions: $numPartitions")
        key.hashCode % numPartitions

      }
    }

    val partitionedStream = shoppingCartEvents.partitionCustom(
      partitioner,
      event => event.userId
    )

    /*
      Bad because
      - you lose parallelism
      - you risk overloading the task with the disproportionate data

      Good for e.g. sending HTTP requests
      DAG : -> Source -> Custom Partitioning -> Print Sink
      Source Task (Subtask 0)
────────────────────────────────────────────────────────
[Generator.run()]
  |
  └─ ctx.collect(ShoppingCartEvent)
         ↓
    ┌────────────────────────────┐
    │ StreamRecord(event + meta)│ ← wrap the event
    └────────────────────────────┘
         ↓
     partitioner: userId → subtask 2
         ↓
 ┌────────────────────────────┐
 │ ResultPartition (buffer)   │ ← serialize → chunked to network
 └────────────────────────────┘
         ↓
  (network shuffle if needed)
         ↓

Sink Task (Subtask 2)
────────────────────────────────────────────────────────
┌────────────────────────────┐
│ InputGate                  │ ← receives byte chunks
└────────────────────────────┘
         ↓
┌────────────────────────────┐
│ RecordDeserializer         │ ← deserialize into StreamRecord
└────────────────────────────┘
         ↓
┌────────────────────────────┐
│ PrintSinkFunction.invoke() │ ← println(event)
└────────────────────────────┘


     */
    //This step is not an operator,
    // but a shuffle instruction between upstream and downstream operators.
    //Flink will apply your Partitioner’s logic
    // to assign each event to a downstream subtask.
    val badPartitioner = new Partitioner[String] {
      override def partition(key: String, numPartitions: Int): Int = { // invoked on every event
        numPartitions - 1 // last partition index
      }
    }

    val badPartitionedStream = shoppingCartEvents.partitionCustom(
      badPartitioner,
      event => event.userId
    )
    // redistribution of data evenly - involves data transfer through network
      .shuffle

     //As there is no Process Function here
    // the PrintSinkFunction is being used as downstream Function to process the event
    badPartitionedStream.print()
    env.execute()
  }


  def main(args: Array[String]): Unit = {
    demoPartitioner()
  }
}
