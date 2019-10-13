package spark.sparkmllib

import org.apache.spark.sql.SparkSession

// 进行物品之间的计算。
object ItemCf {
  /*
     写代码过程之中遇到的小的技巧需要注意：
     多表join 以及 在filter算子的使用，一般在sql之中 filter的使用是挺普遍的。
   */
  def main(args: Array[String]): Unit = {
      val spark=SparkSession.builder().appName("itemCf")
        .getOrCreate()
      import org.apache.spark.sql.functions._
      val df= spark.sql("select * from sl.udata")
      // 这个是在保证每个用户对一个商品只评价一次
      val df_sim=df.selectExpr("user_id ","item_id as item_v","rating as rating_v")
      val df_dot=df.join(df_sim,"user_id")
                   .filter("item_id <>  item_v")
                   .selectExpr("user_id","item_id","item_v"," cast(rating as double) * cast( rating_v as double) as rating")
                   .groupBy("item_id","item_v")
                   .agg(sum("rating").as("rating"))

      val df_end=df.selectExpr("user_id","item_id"," cast(rating as double) *  cast(rating as double)as rating")
                   .groupBy("item_id")
                   .agg(sum("rating").as("rating"))
                   .selectExpr("item_id","sqrt(rating) as rating_s")

      val df_end_sim=df_end.selectExpr("item_id as item_v","rating_s as rating_v")
      // 存储物品之间的相似度
      val df_sim_rel=df_dot.join(df_end,"item_id")
                           .join(df_end_sim,"item_v")
                           .selectExpr("item_id","item_v","rating /(rating_s * rating_v)  as cos")

      //根据物品之间的相似度来求出 需要推荐的物品
      // 获得前10个商品
      val df_topK=df_sim_rel
                  .selectExpr("item_id","item_v","cos","row_number() over(partition by item_id order by cos desc) as rank")
                  .where("rank <=10")

      import  spark.implicits._
      // 获取相似的前十个物品的集合
      val df_list=df_topK.rdd.map(row=>{(row(0).toString,(row(1).toString,row(2).toString))})
        .groupByKey()
        .mapValues(v=>{
                    v.toArray.map(l=>{l._1+"_"+l._2})
        }).toDF("item_id","item_list")

      // 获取用户所有的购买的商品列表
      val df_u=df.rdd.map(row=>{
        (row(0).toString,(row(1).toString,row(2).toString))
      }).groupByKey().mapValues(v=>{
          v.toArray.map(l=>l._1+"_"+l._2)
      }).toDF("user_id","item_l")
     // 用户 物品id 物品列表
     val df_u_item= df.join(df_u,"user_id")
                      .selectExpr("user_id","item_id","rating","item_l")

     spark.udf.register("ff",(item:Seq[String],item2:Seq[String])=>{
       val map= item.map(line=>{
          val l=line.split("_")
          (l(0).toString,l(1).toString.toDouble)
        }).toMap

      val retn= item2.filter(line=>{
          val l=line.split("_")
          map.getOrElse(l(0),-1) == -1
       })
       retn
     })
     // 过滤掉重复的物品
    val df_rec_tmp=df_u_item.join(df_list,"item_id")
       .selectExpr("user_id","rating","ff(item_l,item_list) as rec_list")
       .selectExpr("user_id","rating","explode(rec_list) rec")
       .selectExpr("user_id","split(rec,'_')[0] as item_id","cast(split(rec,'_')[1] as double)*rating as rating")

   // 获得最终的推荐
   val df_rec= df_rec_tmp.groupBy("user_id","item_id")
      .agg(sum("rating").as("rating"))
      .selectExpr("user_id","item_id","rating","row_number() over(partition by user_id order by rating desc) as rank")
      .where("rank<=10")


  }
}
