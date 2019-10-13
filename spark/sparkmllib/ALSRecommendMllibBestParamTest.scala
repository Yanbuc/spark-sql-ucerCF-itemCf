package spark.sparkmllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2016/10/24.
  */
// 获得模型的最好的参数
object ALSRecommendMllibBestParamTest {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ALS_mllib_best_param").setMaster("local").set("spark.sql.warehouse.dir","E:/ideaWorkspace/ScalaSparkMl/spark-warehouse")
    val sc = new SparkContext(conf)
    //设置log基本，生产也建议使用WARN
    Logger.getRootLogger.setLevel(Level.WARN)

    //第一步构建time,Rating
    val movie = sc.textFile("data/mllib/sample_movielens_ratings.txt")
    val ratings = movie.map(line=>{
      val fields = line.split("::")
      val rating  = Rating(fields(0).toInt,fields(1).toInt,fields(2).toDouble)
      val timestamp =fields(3).toLong%5
      (timestamp,rating)

    })

    //输出数据的基本信息
    val numRatings  = ratings.count()
    val numUser  = ratings.map(_._2.user).distinct().count()
    val numItems = ratings.map(_._2.product).distinct().count()
    println("样本基本信息为：")
    println("样本数："+numRatings)
    println("用户数："+numUser)
    println("物品数："+numItems)


    val sp = ratings.randomSplit(Array(0.6,0.2,0.2))
    //第二步骤
    //使用日期把数据分为训练集(timestamp<6),验证集(6<timestamp<8)和测试集（timestamp>8）
    /* val training = ratings.filter(x=>x._1<6).values.repartition(2).cache()
     val validation = ratings.filter(x=>x._1>6 && x._1<8).values.repartition(2).cache()
     val test=ratings.filter(x=>x._1>=8).values.cache()*/
    //样本时间参数都一样，测试就使用随机算了
    val training=sp(0).map(x=>Rating(x._2.user,x._2.product,x._2.rating)).repartition(2).cache()
    val validation=sp(1).map(x=>Rating(x._2.user,x._2.product,x._2.rating)).repartition(2).cache()
    val test=sp(1).map(x=>Rating(x._2.user,x._2.product,x._2.rating))

    val numTraining = training.count()
    val numValidation=validation.count()
    val numTest=test.count()

    println("验证样本基本信息为：")
    println("训练样本数："+numTraining)
    println("验证样本数："+numValidation)
    println("测试样本数："+numTest)


    //第三步
    //定义RMSE方法
    def computeRmse(model:MatrixFactorizationModel,data:RDD[Rating]):Double={
      val predictions:RDD[Rating]=model.predict(data.map(x=>(x.user,x.product)))
      val predictionAndRatings = predictions.map(x=>{((x.user,x.product),x.rating)}).join(data.map(x=>((x.user,x.product),x.rating))).values
      math.sqrt(predictionAndRatings.map(x=>(x._1-x._2)*(x._1-x._2)).mean())
    }

    //第四步骤，使用不同的参数训练模型，并且选择RMSE最小的模型，规定参数的范围
    //隐藏因子数：8或者12
    //正则化系数，0.01或者0.1选择，迭代次数为10或者20,训练8个模型
    val ranks = List(8,12)
    val lambdas = List(0.01,0.1)
    val numiters = List(10,20)
    var bestModel:Option[MatrixFactorizationModel]=None
    var bestValidationRmse=Double.MaxValue
    var bestRank=0
    var bestLamdba = -1.0
    var bestNumIter=1
    for(rank<-ranks;lambda<-lambdas;numiter<-numiters){
      println(rank+"-->"+lambda+"-->"+numiter)
      val model = ALS.train(training,rank,numiter,lambda)
      val valadationRmse=computeRmse(model,validation)
      if(valadationRmse<bestValidationRmse){
        bestModel=Some(model)
        bestValidationRmse=valadationRmse
        bestRank=rank
        bestLamdba=lambda
        bestNumIter=numiter
      }
    }

    val testRmse = computeRmse(bestModel.get,test)
    println("测试数据的rmse为："+testRmse)
    println("范围内的最后模型参数为：")
    println("隐藏因子数："+bestRank)
    println("正则化参数："+bestLamdba)
    println("迭代次数："+bestNumIter)
    //步骤5可以对比使用协同过滤和不适用协同过滤（使用平均分来做预测结果）能提升多大的预测效果。

    //计算训练样本和验证样本的平均分数
    val meanR = training.union(validation).map(x=>x.rating).mean()

    //这就是使用平均分做预测，test样本的rmse
    val baseRmse=math.sqrt(test.map(x=>(meanR-x.rating)*(meanR-x.rating)).mean())

    val improvement =(baseRmse-testRmse)/baseRmse*100

    println("使用了ALS协同过滤算法比使用评价分作为预测的提升度为："+improvement)

  }
}

