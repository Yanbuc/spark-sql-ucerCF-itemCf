package spark.sparksql


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

case class Dura(pcode:Int,event_date:String,duration:Int)
object WindowFunction {
  def main(args: Array[String]): Unit = {

    val conf=new SparkConf().setMaster("local[*]").setAppName("window function test")
      .set("spark.sql.warehouse.dir","D://spark/ware")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val rdd= spark.sparkContext.textFile("D://spark/data/time.txt")
    import spark.implicits._
    import org.apache.spark.sql.expressions._
    import org.apache.spark.sql.functions._
    val df=rdd.map(line=>line.split(","))
      .map(fileds=>Dura(fileds(0).toInt,fileds(1),fileds(2).toInt)).toDF()
    df.createOrReplaceTempView("user")
    /*
   、
    spark.sql("""
        select pcode,event_date,sum(duration) over(partition by pcode order by event_date)
        as sum_duration from user
      """).show()
    println("the next is dataFrame use window function ")
    var window=Window.partitionBy("pcode")
      .orderBy("event_date")
    df.select($"pcode",$"event_date",sum($"duration").over(window)
      .as("sum_duration"))
      .show()
    println("calculate before n days")
    spark.sql(
      """
        select pcode,event_date,sum(duration) over(partition by pcode order by event_date asc rows between unbounded preceding and current row )
                as sum_duration from user
      """).show()
    println("use dataframe calculate before n days")
     window=Window.partitionBy("pcode")
      .orderBy("event_date")
    df.select($"pcode",$"event_date",sum($"duration").over(window).as("sumDuration"))
      .show()
    */
    println("累加前三天 sql 版本")
    spark.sql(
      """
        | select pcode,event_date,sum(duration) over(partition by pcode order by event_date asc rows between 3 preceding and current row )
        |                as sum_duration from user
      """.stripMargin).show()
    println("累加前三天 dataFrame版本")
  //  var window=Window.partitionBy("pcode").orderBy("event_date").rangeBetween(-3,0)
    //df.select($"pcode",$"event_date",sum($"duration").over(window).as("sum_duration")).show()
    var window=Window.partitionBy("pcode")
      .orderBy("event_date").rowsBetween(-3,0)
    df.select($"pcode",$"event_date",sum($"duration").over(window)
      .as("sum_duration"))
      .show()

    // 使用roll up 统计全部


  }
}
