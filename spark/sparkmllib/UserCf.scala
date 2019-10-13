package spark.sparkmllib

import org.apache.spark.sql.SparkSession
/*
   这里的代码的书写过程之中的
 */
class UserCf {
  // usercf 实现
  def main(args: Array[String]): Unit = {
       val spark=SparkSession.builder().appName("user base").enableHiveSupport()
         .getOrCreate()
       import  org.apache.spark.sql.functions._
       import spark.implicits._
      // 计算点乘的和
       val df= spark.sql("select * from sl.udata")
       val df_sim=df.selectExpr("user_id as user_v","item_id as item_id","rating as rating_v")
       // 建立item-user倒排表
       val df_join=df.join(df_sim,"item_id").filter("cast(user_id as long) <> cast(user_v as long)")
       val df_dot=df_join.selectExpr("user_id","user_v","cast(rating as long)  * cast(rating_v as long) as rating_sum")
         .groupBy("user_id","user_v")
         .agg(sum("rating_sum").as("rating_s"))
       // 计算每个用户的模的和
       val df_score_tmp=df.selectExpr("user_id","cast(rating as long) * cast(rating as long) as rating_s")
         .groupBy("user_id")
         .agg(sum("rating_s").as("rating_sum"))
       val df_score=df_score_tmp.selectExpr("user_id as user_c","sqrt(rating_sum) as rating_sum")

       // 计算求出不同用户之间的相似度
      val df_sim_tmp= df_dot.join(df_score,df_dot("user_id")===df_score("user_c"))
        .selectExpr("user_id","user_v","rating_s","rating_sum as rating_a")
      val df_sim_rel= df_sim_tmp.join(df_score,df_sim_tmp("user_v")===df_score("user_c"))
        .selectExpr("user_id","user_v","rating_s/(rating_a*rating_sum)  as cos")

      // 获得最大的用户的十个相似度
      val k=10
      val df_user_topK=df_sim_rel.selectExpr("user_id","user_v","cos","row_number() over(partition by user_id order by cos desc) as rank ")
      .where("rank<="+k)

      // 获取用户的物品集合
     val user_item_list= df.rdd.map(row=>{
        (row(0).toString,(row(1).toString+"_"+row(2).toString))
      }).groupByKey().mapValues(x=>x.toArray).toDF("user_id","item_list")

     // 对用户获得的物品集合进行去重
     val user_item_list_v=user_item_list
       .selectExpr("user_id as user_v","item_list as item_list_v")
     val removeSimi=udf((item1:Seq[String],item2:Seq[String]) =>{
       val map= item1.map(x=>{
         val l=x.split("_")
         (l(0).toString,l(1).toDouble)
       }).toMap
       val retn= item2.filter(line=>{
         val l=line.split("_")
         map.getOrElse(l(0).toString,-1) == -1
       })
       retn
     })
    spark.udf.register("removeSimi",(item1:Seq[String],item2:Seq[String]) =>{
      val map= item1.map(x=>{
        val l=x.split("_")
        (l(0).toString,l(1).toDouble)
      }).toMap
      val retn= item2.filter(line=>{
        val l=line.split("_")
        map.getOrElse(l(0).toString,-1) == -1
      })
      retn
    })
    val user_dis_item=df_user_topK.join(user_item_list,"user_id")
      .join(user_item_list_v,"user_v")
      .selectExpr("user_id","cast(`cos` as double) as `cos`","removeSimi(item_list,item_list_v) as items")

    // 计算物品的评分
    val calItemRating=udf((cos:Double,items:Seq[String])=>{
          val tu= items.map(line=>{
               val l=line.split("_")
              (l(0).toString,l(1).toString.toDouble* cos)
           })
          tu
    })
    spark.udf.register("calItemRating",(cos:Double,items:Seq[String])=>{
      val tu= items.map(line=>{
        val l=line.split("_")
        (l(0).toString,l(1).toString.toDouble* cos)
      })
      tu
    })
    // 对用户进行推荐
   val user_rec_tmp= user_dis_item.selectExpr("user_id","calItemRating(cos,items) as items").selectExpr("user_id","explode(items) as tu").selectExpr("user_id","tu._1 as item_id","tu._2 as rating")
      .groupBy("user_id","item_id")
      .agg(sum("rating").as("rating"))
    // 得出前10个推荐商品
  val user_rec=  user_rec_tmp.selectExpr("user_id","item_id","row_number() over(partition by user_id order by rating desc) as rank")
      .where("rank<="+k)

  }
}
