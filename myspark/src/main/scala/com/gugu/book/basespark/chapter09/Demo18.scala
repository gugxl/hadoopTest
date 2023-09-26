package com.gugu.book.basespark.chapter09

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo18 {
    case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)
    
    def parseRating(str: String): Rating = {
        val fields = str.split("::")
        assert(fields.size == 4)
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
        
    }
    
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("Demo18").master("local").getOrCreate()
        import spark.implicits._
        val ratings: DataFrame = spark.sparkContext
            .textFile("file:///D:\\applicationfiles\\data\\sample_movielens_ratings.txt")
            .map(parseRating)
            .toDF()
        ratings.show()
        
        val  Array(training,test) = ratings.randomSplit(Array(0.8, 0.2))
        val alsExplicit: ALS = new ALS()
            .setMaxIter(5)
            .setRegParam(0.01)
            .setUserCol("userId")
            .setItemCol("movieId")
            .setRatingCol("rating")
        
        val alsImplicit: ALS = new ALS()
            .setMaxIter(5)
            .setRegParam(0.01)
            .setImplicitPrefs(true)
            .setUserCol("userId")
            .setItemCol("movieId")
            .setRatingCol("rating")
        val modelExplicit: ALSModel = alsExplicit.fit(training)
        val modelImplicit: ALSModel = alsImplicit.fit(training)
        
        val predictionsExplicit: DataFrame = modelExplicit.transform(test).na.drop()
        val predictionsImplicit: DataFrame = modelImplicit.transform(test).drop()
        predictionsExplicit.show()
        predictionsImplicit.show()
        
        val evaluator: RegressionEvaluator = new RegressionEvaluator()
            .setMetricName("rmse")
            .setLabelCol("rating")
            .setPredictionCol("prediction")
        
        val rmseExplicit: Double = evaluator.evaluate(predictionsExplicit)
        val rmseImplicit: Double = evaluator.evaluate(predictionsImplicit)
        
        println(rmseExplicit)
        println(rmseExplicit)
        
        
    }
    
}
