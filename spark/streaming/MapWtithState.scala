package spark.streaming
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable.Set

/**
  * Created by lgh on 2017/8/24.
  */
object mapWithState {
  def main(args: Array[String]): Unit = {
    def main(args: Array[String]): Unit = {
      val brokers = "mtime-bigdata00:9092,mtime-bigdata01:9092";
      val topics = "testkafka";
      val batchseconds = "10";
      val checkpointDirectory = "./mapwithstate";
      val ssc = StreamingContext.getOrCreate(checkpointDirectory, () => createcontext(brokers, topics, batchseconds, checkpointDirectory))
      ssc.start()
      ssc.awaitTermination()
    }


    def createcontext(brokers: String, topics: String, batchseconds: String, checkpointDirectory: String): StreamingContext = {
      val sparkconf = new SparkConf().setAppName("TestUpStateByKey").setMaster("local[3]")
      val ssc = new StreamingContext(sparkconf, Seconds(batchseconds.toInt))
      val topicsSet = topics.split(",").toSet
      val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)


      val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

      val lines: DStream[String] = messages.map(_._2)

      val message: DStream[(String, Long)] = lines.flatMap(_.split(" ")).map(x => (x, 1l)).reduceByKey(_ + _)

      //匿名函数
      val logical = (key: String, value: Option[Long], state: State[Long]) => {
        //这个的作用是检测已经过期的key并移除；如果有key过期后又有这个key新的数据进来，不加isTimeout的话就会导致报错
        if (state.isTimingOut()) {
          System.out.println(key + " is timingout")
        }
        else {
          val sum = state.getOption().getOrElse(0l) + value.getOrElse(0l)
          val output = (key, sum)
          //更新状态
          state.update(sum)

          output
        }
      }

      val keyvalue = message.mapWithState(StateSpec.function(logical).timeout(Seconds(60)))


      keyvalue.stateSnapshots().foreachRDD((rdd, time) => {
        println("========" + rdd.count())
        rdd.foreach(x => println(x._1 + "=" + x._2)
        )
      })

      ssc.checkpoint(checkpointDirectory)
      ssc
    }
  }
}