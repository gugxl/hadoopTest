package com.gugu.book.basespark.chapter09

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

//决策树分类器
object Demo13 {
    // 特征选择、决策树的生成和决策树的剪枝
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("Demo13").master("local").getOrCreate()
        import spark.implicits._
        val data: DataFrame = spark
            .sparkContext
            .textFile("file:///D:\\applicationfiles\\data\\iris.data.txt")
            .map(_.split(","))
            .map(p => Iris(Vectors.dense(
                p(0).toDouble,
                p(1).toDouble,
                p(2).toDouble,
                p(3).toDouble),
                p(4)))
            .toDF()
        
        val labelIndexer: StringIndexerModel = new StringIndexer()
            .setInputCol("label")
            .setOutputCol("indexedLabel")
            .fit(data)
        
        val featureIndexer: VectorIndexerModel = new VectorIndexer()
            .setInputCol("features")
            .setOutputCol("indexedFeatures")
            .setMaxCategories(4)
            .fit(data)
        
        val labelConverter: IndexToString = new IndexToString()
            .setInputCol("prediction")
            .setOutputCol("predictedLabel")
            .setLabels(labelIndexer.labels)
        
        val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
        
        val dtClassifier: DecisionTreeClassifier = new DecisionTreeClassifier()
            .setLabelCol("indexedLabel")
            .setFeaturesCol("indexedFeatures")
        
        val lrPipeline: Pipeline = new Pipeline()
            .setStages(Array(labelIndexer, featureIndexer, dtClassifier, labelConverter))
        
        val lrPipelineModel: PipelineModel = lrPipeline.fit(trainingData)
        
        val dtPredictions: DataFrame = lrPipelineModel.transform(testData)
        dtPredictions.select("predictedLabel", "label", "features").show(100)
        
        val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("indexedLabel")
            .setPredictionCol("prediction")
        
        val dtAccuracy: Double = evaluator.evaluate(dtPredictions)
        println(dtAccuracy)
        
        val treeModelClassifier: DecisionTreeClassificationModel = lrPipelineModel
            .stages(2)
            .asInstanceOf[DecisionTreeClassificationModel]
        println("Learned classification tree model:\n" + treeModelClassifier.toDebugString)
    }
    
    case class Iris(features: org.apache.spark.ml.linalg.Vector, label: String)
    
}
