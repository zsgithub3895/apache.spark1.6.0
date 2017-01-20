package com.sihuatech.sqm.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

/**
 * Example for LogisticRegressionWithLBFGS.
 */
public class JavaLogisticRegressionWithLBFGSExample {// 拟牛顿法
	static SparkConf conf = new SparkConf().setAppName("JavaLogisticRegressionWithLBFGSExample");
	static SparkContext sc = new SparkContext(conf);

	public static void main(String[] args) {
	String path = "data/mllib/people3.txt";
	String pathTwo = "data/mllib/testData.txt";
	String pathThree = "data/mllib/testData2.txt";
	String pathFour = "data/mllib/testData3.txt";
	
	//JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(sc, pathOne).toJavaRDD();
	JavaRDD<LabeledPoint> data  = MLUtils.loadLabeledData(sc, path).toJavaRDD();
	JavaRDD<LabeledPoint> dataTwo = MLUtils.loadLabeledData(sc, pathTwo).toJavaRDD();
	JavaRDD<LabeledPoint> dataThree = MLUtils.loadLabeledData(sc, pathThree).toJavaRDD();
	JavaRDD<LabeledPoint> dataFour = MLUtils.loadLabeledData(sc, pathFour).toJavaRDD();
	JavaRDD<LabeledPoint> training = data;
	JavaRDD<LabeledPoint> test = dataTwo;
	JavaRDD<LabeledPoint> testTwo = dataThree;
	JavaRDD<LabeledPoint> testThree = dataFour;
	// Run training algorithm to build the model.
	final LogisticRegressionModel model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(training.rdd());

	JavaRDD<Tuple2<Object, Object>> predictionAndLabels = test.map(new Function<LabeledPoint, Tuple2<Object, Object>>() {
				public Tuple2<Object, Object> call(LabeledPoint p) {
					Double prediction = model.predict(p.features());
					return new Tuple2<Object, Object>(prediction, p.label());
				}
			});
	
	JavaRDD<Tuple2<Object, Object>> predictionAndLabelsTwo = testTwo.map(new Function<LabeledPoint, Tuple2<Object, Object>>() {
		public Tuple2<Object, Object> call(LabeledPoint p) {
			Double prediction = model.predict(p.features());
			return new Tuple2<Object, Object>(prediction, p.label());
		}
	});
	
	JavaRDD<Tuple2<Object, Object>> predictionAndLabelsThree = testThree.map(new Function<LabeledPoint, Tuple2<Object, Object>>() {
		public Tuple2<Object, Object> call(LabeledPoint p) {
			Double prediction = model.predict(p.features());
			return new Tuple2<Object, Object>(prediction, p.label());
		}
	});
	//metrics：指标     matrix：矩阵，模型
	MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());
	MulticlassMetrics metricsTwo = new MulticlassMetrics(predictionAndLabelsTwo.rdd());
	MulticlassMetrics metricsThree = new MulticlassMetrics(predictionAndLabelsThree.rdd());
	System.out.println("precision = " + metrics.precision());
	//System.out.println("++++++++++++++++++="+Arrays.toString(metrics.labels()));
	//System.out.println("precision2 = " + metricsTwo.precision());
	//System.out.println("++++++++++++++++++="+Arrays.toString(metricsTwo.labels()));
	//System.out.println("precision3 = " + metricsThree.precision());
	//System.out.println("++++++++++++++++++="+Arrays.toString(metricsThree.labels()));
    sc.stop();
	}
}
