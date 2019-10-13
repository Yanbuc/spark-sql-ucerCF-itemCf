package spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object WordCountUseMapWithState {
  def main(args: Array[String]): Unit = {
      val conf=new SparkConf().setMaster("local[*]").setAppName("wordcount use mapWithState")
      val ssc=new StreamingContext(conf,Seconds(2))
      ssc.checkpoint("D://spark/check")
      ssc.sparkContext.setLogLevel("ERROR")
      val lines = ssc.socketTextStream("192.168.249.10",8999)
      val message: DStream[(String, Long)] = lines.flatMap(line=>
        line.trim.split("\\s+")).map(wd=>(wd,1L))
      val mapFunc=(word:String,v:Option[Long],state: State[Long])=>{
          val sum=v.getOrElse(0L)+state.getOption().getOrElse(0L)
          state.update(sum)
          val output=(word,sum)
           output
      }
    val map: MapWithStateDStream[String, Long, Long, (String, Long)] = message.mapWithState(StateSpec.function(mapFunc))
    map.foreachRDD(rdd=>{
       rdd.foreach(line=>{
         if(line._2==1){
           println("he is a new boy")
         }else if(line._2>=2){
           println("he is a old boy")
         }

       })
    })
    ssc.start()
    ssc.awaitTermination()


  }
}
