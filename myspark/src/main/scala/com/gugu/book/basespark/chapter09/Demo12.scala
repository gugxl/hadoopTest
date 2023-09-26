package com.gugu.book.basespark.chapter09

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Demo12 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().appName("Demo12").master("local").getOrCreate()
        val path = "file:///D:\\applicationfiles\\data\\iris.data.txt"
        val df_raw: DataFrame = spark.read
            .option("inferSchema", true)
            .csv(path)
            .toDF("c0", "c1", "c2", "c3", "label")
        val df_double: DataFrame = df_raw.select(
            col("c0").cast(DoubleType),
            col("c1").cast(DoubleType),
            col("c2").cast(DoubleType),
            col("c3").cast(DoubleType),
            col("label"))
        val assembler: VectorAssembler = new VectorAssembler()
            .setInputCols(Array("c0", "c1", "c2", "c3"))
            .setOutputCol("features")
        val data: DataFrame = assembler.transform(df_double).select("features", "label")
        data.show()
        
        val labelIndexer: StringIndexerModel = new StringIndexer()
            .setInputCol("label")
            .setOutputCol("indexedLabel")
            .fit(data)
        val featureIndexer: VectorIndexerModel = new VectorIndexer()
            .setInputCol("features")
            .setOutputCol("indexedFeatures")
            .fit(data)
        
        //        逻辑斯蒂回归分类器
        val lr: LogisticRegression = new LogisticRegression()
            .setLabelCol("indexedLabel")
            .setFeaturesCol("indexedFeatures")
            .setMaxIter(100)
            .setRegParam(0.3)
            .setElasticNetParam(0.8)
        
        println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")
        
        val labelConverter: IndexToString = new IndexToString()
            .setInputCol("prediction")
            .setOutputCol("predictedLabel")
            .setLabels(labelIndexer.labels)
        
        val lrPipeline: Pipeline = new Pipeline()
            .setStages(Array(labelIndexer, featureIndexer, lr, labelConverter))
        
        val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
        val lrPipelineModel: PipelineModel = lrPipeline.fit(trainingData)
        val lrPredictions: DataFrame = lrPipelineModel.transform(testData)
        lrPredictions
            .select("predictedLabel", "label", "features", "probability")
            .collect()
            .foreach {
                case Row(predictedLabel, label, features, prob)
                =>
                    println(s"($label, $features) --> prob=$prob, predicted Label=$predictedLabel")
            }
        //训练的模型进行评估
        val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("indexedLabel")
            .setPredictionCol("prediction")
        
        val lrAccuracy: Double = evaluator.evaluate(lrPredictions)
        println(lrAccuracy)
        val lrModel = lrPipelineModel
            .stages(2)
            .asInstanceOf[LogisticRegressionModel]
        
        
        println("Coefficients: \n " + lrModel.coefficientMatrix
            + "\nIntercept: " + lrModel.interceptVector
            + "\n numClasses: " + lrModel.numClasses
            + "\n numFeatures: " + lrModel.numFeatures)
        
    }
    
    
}
