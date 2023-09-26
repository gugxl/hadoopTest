package com.gugu.book.basespark.chapter09

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Demo19 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().master("local").appName("Demo19").getOrCreate()
        val path = "file:///D:\\applicationfiles\\data\\iris.data.txt"
        val df_raw: DataFrame = spark.read.option("inferSchema", true)
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
        
        val data: DataFrame = assembler
            .transform(df_double)
            .select("features", "label")
        
        val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))
        val labelIndexer: StringIndexerModel = new StringIndexer()
            .setInputCol("label")
            .setOutputCol("indexedLabel")
            .fit(data)
        
        val featureIndexer: VectorIndexerModel = new VectorIndexer()
            .setInputCol("features")
            .setOutputCol("indexedFeatures")
            .fit(data)
        
        val lr: LogisticRegression = new LogisticRegression()
            .setLabelCol("indexedLabel")
            .setFeaturesCol("indexedFeatures")
            .setMaxIter(50)
        
        val labelConverter: IndexToString = new IndexToString()
            .setInputCol("prediction")
            .setOutputCol("predictedLabel")
            .setLabels(labelIndexer.labels)
        
        val lrPipeline: Pipeline = new Pipeline()
            .setStages(Array(labelIndexer, featureIndexer, lr, labelConverter))
        
        val paramGrid: Array[ParamMap] = new ParamGridBuilder()
            .addGrid(lr.elasticNetParam, Array(0.2, 0.8))
            .addGrid(lr.regParam, Array(0.01, 0.1, 0.5))
            .build()
        
        println(paramGrid)
        
        val cv: CrossValidator = new CrossValidator()
            .setEstimator(lrPipeline)
            .setEvaluator(new MulticlassClassificationEvaluator()
                .setLabelCol("indexedLabel")
                .setPredictionCol("prediction"))
            .setEstimatorParamMaps(paramGrid)
            .setNumFolds(3)
        val cvModel: CrossValidatorModel = cv.fit(trainingData)
        val lrPredictions: DataFrame = cvModel.transform(testData)
        lrPredictions.select("predictedLabel", "label", "features", "probability").show(20)
        lrPredictions
            .select("predictedLabel", "label", "features", "probability")
            .collect()
            .foreach {
                case Row(predictedLabel: String, label: String, features: Vector, prob: Vector) =>
                    println(s"($label, $features)-->prob=$prob, predicted Label=$predictedLabel")
            }
        val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("indexedLabel")
            .setPredictionCol("prediction")
        
        val lrAccuracy: Double = evaluator.evaluate(lrPredictions)
        println(lrAccuracy)
        
        val bestModel: PipelineModel = cvModel.bestModel.asInstanceOf[PipelineModel]
        
        val lrModel: LogisticRegressionModel = bestModel.stages(2)
            .asInstanceOf[LogisticRegressionModel]
        
        println(
            "Coefficients: " + lrModel.coefficientMatrix +
                "Intercept: " + lrModel.interceptVector +
                "numClasses: " + lrModel.numClasses +
                "numFeatures: " + lrModel.numFeatures)
        
        lrModel.explainParam(lrModel.regParam)
        lrModel.explainParam(lrModel.elasticNetParam)
    }
    
}
