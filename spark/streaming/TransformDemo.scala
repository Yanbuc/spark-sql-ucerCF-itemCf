package spark.streaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds

object TransformaDemo {
  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setMaster("local[2]").setAppName("TransformaDemo")
    val  ssc=new StreamingContext(conf,Seconds(5))
    val fileDS=ssc.socketTextStream("192.168.32.110", 9999)
    val wordcountDS=fileDS.flatMap { line => line.split("\t") }
      .map { word => (word,1) }

    /**
      * 假设这个是黑名单
      */
    val fillter=ssc.sparkContext.parallelize(List(",","?","!",".")).map { param => (param,true) }

    val needwordDS=  wordcountDS.transform( rdd =>{
      val leftRDD=  rdd.leftOuterJoin(fillter)
      //leftRDD String,(int,option[boolean]);
      val needword=leftRDD.filter( tuple =>{
        val x= tuple._1;
        val y=tuple._2;
        if(y._2.isEmpty){
          true;
        }else{
          false;
        }

      })
      needword.map(tuple =>(tuple._1,1))

    })

    val wcDS= needwordDS.reduceByKey(_+_)
    wcDS.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
