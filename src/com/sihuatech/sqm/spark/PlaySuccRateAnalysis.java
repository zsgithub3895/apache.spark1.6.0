package com.sihuatech.sqm.spark;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import com.sihuatech.sqm.spark.bean.PlayFailLog;
import com.sihuatech.sqm.spark.util.DateUtil;
import com.sihuatech.sqm.spark.util.PropHelper;

public class PlaySuccRateAnalysis {

	private static Logger logger = Logger.getLogger(PlaySuccRateAnalysis.class);
	private static HashMap<String, Long> hm = new HashMap<String, Long>();
	private static final Pattern TAB = Pattern.compile("\t");

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		if (null == args || args.length < 4) {
			System.err
					.println("Usage: PlaySuccRateAnalysis <zkQuorum> <group> <topics> <numThreads>");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf()
				.setAppName("PlaySuccRateAnalysis");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		String PLAY_SUCC_TIME = PropHelper.getProperty("PLAY_SUCC_TIME");

		if (PLAY_SUCC_TIME != null && !PLAY_SUCC_TIME.equals("")) {
			JavaStreamingContext jssc = new JavaStreamingContext(ctx,
					Durations.seconds(Long.valueOf(PLAY_SUCC_TIME)));

			// 此参数为接收Topic的线程数，并非Spark分析的分区数
			int numThreads = Integer.parseInt(args[3]);
			Map<String, Integer> topicMap = new HashMap<String, Integer>();
			logger.info("开始读取内容：");
			String[] topics = args[2].split(",");
			for (String topic : topics) {
				logger.info("topic：" + topic);
				topicMap.put(topic, numThreads);
			}
			JavaPairReceiverInputDStream<String, String> messages = KafkaUtils
					.createStream(jssc, args[0], args[1], topicMap);
			JavaDStream<String> lines = messages
					.map(new Function<Tuple2<String, String>, String>() {
						
						private static final long serialVersionUID = 1L;

						@Override
						public String call(Tuple2<String, String> tuple2) {
							logger.info("++++++++-----------++[播放失败日志]"
									+ tuple2._2);
							return tuple2._2();
						}
					});
			// 校验日志，过滤不符合条件的记录
			JavaDStream<String> filterLines = lines
					.filter(new Function<String, Boolean>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Boolean call(String line) throws Exception {
							String[] lineArr = line.split("\\|", -1);
							if (lineArr.length < 7) {
								return false;
							} else if (!"5".equals(lineArr[0])) {
								return false;
							} else if ("".equals(lineArr[1])) {
								return false;
							} else if ("".equals(lineArr[2])) {
								return false;
							} else if ("".equals(lineArr[3])) {
								return false;
							} else if ("".equals(lineArr[4])) {
								return false;
							} else if ("".equals(lineArr[5])) {
								return false;
							} else if ("".equals(lineArr[6])) {
								return false;
							} else if ("".equals(lineArr[7])) {
								return false;
							} else if ("".equals(lineArr[8])) {
								return false;
							} else if ("".equals(lineArr[9])) {
								return false;
							} else if ("".equals(lineArr[10])) {
								return false;
							} else if ("".equals(lineArr[11])) {
								return false;
							} else if ("".equals(lineArr[12])) {
								return false;
							} else if ("".equals(lineArr[13])) {
								return false;
							} else if ("".equals(lineArr[14])) {
								return false;
							}
							return true;
						}
					});
			
			filterLines
					.foreachRDD(new Function2<JavaRDD<String>, Time, Void>() {

						private static final long serialVersionUID = 1L;

						@Override
						public Void call(JavaRDD<String> rdd, Time time) {
							String[] dimension = { "provinceID", "platform",
									"deviceProvider", "fwVersion" };
							SQLContext sqlContext = JavaSQLContextSingleton
									.getInstance(rdd.context());
							// Convert JavaRDD[String] to JavaRDD[bean class] to
							JavaRDD<PlayFailLog> rowRDD = rdd
									.map(new Function<String, PlayFailLog>() {
										private static final long serialVersionUID = 1L;

										public PlayFailLog call(String word)
												throws Exception {
											PlayFailLog playFailLog = null;
											if (StringUtils.isNotBlank(word)) {
												String[] fields = word.split(
														"\\|", -1);
												playFailLog = new PlayFailLog();
												playFailLog
														.setDeviceProvider(fields[2]);
												playFailLog
														.setPlatform(fields[3]);
												playFailLog
														.setProvinceID(fields[4]);
												playFailLog
														.setFwVersion(fields[6]);
												playFailLog
														.setStartSecond(fields[7]);
											}
											return playFailLog;
										}
									});

							DataFrame wordsDataFrame = sqlContext
									.createDataFrame(rowRDD, PlayFailLog.class);
							wordsDataFrame.registerTempTable("PlayFailLog");

							/** 维度不包含ALL *********************************************************************/
							noALLByDimension(sqlContext, dimension);

							/** 维度中包含ALL ******************************************************************/
							differentALLByDimension(sqlContext, dimension);

							save();
							return null;
						}
					});
			jssc.start();
			jssc.awaitTermination();
		}
	}

	public static void save() {
		if (hm != null && hm.size() > 0) {
			IndexToRedis.playSuccToRedis(hm);
			// IndexToMysql.toMysql(hm);
			hm.clear();
		}
	}

	public static void noALLByDimension(SQLContext sqlContext,
			String[] dimension) {
		DataFrame noALLDimensionToTimes = sqlContext.sql("select count(*),"
				+ dimension[0] + "," + dimension[1] + "," + dimension[2] + ","
				+ dimension[3] + " from PlayFailLog group by " + dimension[0]
				+ "," + dimension[1] + "," + dimension[2] + "," + dimension[3]);
		JavaRDD<String> noALLNamesToTimes = noALLDimensionToTimes.toJavaRDD()
				.map(new Function<Row, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public String call(Row row) {
						String time = DateUtil.getCurrentDateTime("yyyyMMddHHmm");
						String kk = row.getString(1) + "#" + row.getString(2)
								+ "#" + row.getString(3) + "#"
								+ row.getString(4) + "#" + time + "\t" + row.getLong(0);
						return kk;
					}
				});

		List<String> stateRow = noALLNamesToTimes.collect();
		if (stateRow == null || stateRow.size() == 0) {
		} else {
			for (String row : stateRow) {
				String[] rowArr = TAB.split(row);
				hm.put(rowArr[0], Long.valueOf(rowArr[1]));
			}
		}
	}

	public static void differentALLByDimension(SQLContext sqlContext,
			String[] dimension) {
		/** 四个维度为ALL */
		fourALLDimensions(sqlContext);

		/** 三个维度为ALL */
		for (int i = 0; i < dimension.length; i++) {
			StringBuffer sb = new StringBuffer();
			sb.append("select count(*)," + i + "," + dimension[i]
					+ " from PlayFailLog group by " + dimension[i]);
			DataFrame dAllToTimes = sqlContext.sql(sb.toString());
			dAllToTimes.show();
			JavaRDD<String> oneAllNamesToTimes = dAllToTimes.toJavaRDD().map(
					new Function<Row, String>() {
						private static final long serialVersionUID = 1L;

						@Override
						public String call(Row row) {
							String kk = null;
							String time = DateUtil.getCurrentDateTime("yyyyMMddHHmm");
							if (row.getInt(1) == 0) {
								kk = row.getString(2) + "#ALL#ALL#ALL#" + time + "\t"
										+ row.getLong(0);
							}
							if (row.getInt(1) == 1) {
								kk = "ALL#" + row.getString(2) + "#ALL#ALL#"
										+ time + "\t" + row.getLong(0);
							}
							if (row.getInt(1) == 2) {
								kk = "ALL#ALL#" + row.getString(2) + "#ALL#"
										+ time + "\t" + row.getLong(0);
							}
							if (row.getInt(1) == 3) {
								kk = "ALL#ALL#ALL#" + row.getString(2) + "#" + time + "\t"
										+ row.getLong(0);
							}
							return kk;
						}
					});

			List<String> stateRow = oneAllNamesToTimes.collect();
			if (stateRow == null || stateRow.size() == 0) {
			} else {
				for (String row : stateRow) {
					String[] rowArr = TAB.split(row);
					hm.put(rowArr[0], Long.valueOf(rowArr[1]));
				}
			}

			/** 两个维度为ALL */
			for (int j = i + 1; j < dimension.length; j++) {
				sb = new StringBuffer();
				sb.append("select count(*)," + i + j + "," + dimension[i] + ","
						+ dimension[j] + " from PlayFailLog group by "
						+ dimension[i] + "," + dimension[j]);

				DataFrame ddAllToTimes = sqlContext.sql(sb.toString());
				ddAllToTimes.show();
				JavaRDD<String> twoAllNamesToTimes = ddAllToTimes.toJavaRDD()
						.map(new Function<Row, String>() {
							private static final long serialVersionUID = 1L;

							@Override
							public String call(Row row) {
								String kk = null;
								String time = DateUtil.getCurrentDateTime("yyyyMMddHHmm");
								if (row.getInt(1) == 1) {
									kk = row.getString(2) + "#"
											+ row.getString(3) + "#ALL#ALL#"
											+ time + "\t" + row.getLong(0);
								}
								if (row.getInt(1) == 2) {
									kk = row.getString(2) + "#ALL#"
											+ row.getString(3) + "#ALL#" + time + "\t"
											+ row.getLong(0);
								}
								if (row.getInt(1) == 3) {
									kk = row.getString(2) + "#ALL#ALL#"
											+ row.getString(3) + "#" + time + "\t"
											+ row.getLong(0);
								}
								if (row.getInt(1) == 12) {
									kk = "ALL#" + row.getString(2) + "#"
											+ row.getString(3) + "#ALL#" + time + "\t"
											+ row.getLong(0);
								}
								if (row.getInt(1) == 13) {
									kk = "ALL#" + row.getString(2) + "#ALL#"
											+ row.getString(3) + "#" + time + "\t"
											+ row.getLong(0);
								}
								if (row.getInt(1) == 23) {
									kk = "ALL#ALL#" + row.getString(2) + "#"
											+ row.getString(3) + "#" + time + "\t"
											+ row.getLong(0);
								}
								return kk;
							}
						});

				List<String> stateRow1 = twoAllNamesToTimes.collect();
				if (stateRow1 == null || stateRow1.size() == 0) {
				} else {
					for (String row : stateRow1) {
						String[] rowArr = TAB.split(row);
						hm.put(rowArr[0], Long.valueOf(rowArr[1]));
					}
				}

				/** 一个维度为ALL */
				for (int k = j + 1; k < dimension.length; k++) {
					sb = new StringBuffer();
					sb.append("select count(*)," + i + j + k + ","
							+ dimension[i] + "," + dimension[j] + ","
							+ dimension[k] + " from PlayFailLog group by "
							+ dimension[i] + "," + dimension[j] + ","
							+ dimension[k]);
					DataFrame dddAllToTimes = sqlContext.sql(sb.toString());
					dddAllToTimes.show();
					JavaRDD<String> thirdAllNamesToTimes = dddAllToTimes
							.toJavaRDD().map(new Function<Row, String>() {
								private static final long serialVersionUID = 1L;

								@Override
								public String call(Row row) {
									String kk = null;
									String time = DateUtil.getCurrentDateTime("yyyyMMddHHmm");
									if (row.getInt(1) == 12) {
										kk = row.getString(2) + "#"
												+ row.getString(3) + "#"
												+ row.getString(4) + "#ALL#"
												+ time + "\t" + row.getLong(0);
									}
									if (row.getInt(1) == 13) {
										kk = row.getString(2) + "#"
												+ row.getString(3) + "#ALL#"
												+ row.getString(4) + "#" + time + "\t"
												+ row.getLong(0);
									}
									if (row.getInt(1) == 23) {
										kk = row.getString(2) + "#ALL#"
												+ row.getString(3) + "#"
												+ row.getString(4) + "#" + time + "\t"
												+ row.getLong(0);
									}
									if (row.getInt(1) == 123) {
										kk = "ALL#" + row.getString(2) + "#"
												+ row.getString(3) + "#"
												+ row.getString(4) + "#" + time + "\t"
												+ row.getLong(0);
									}
									return kk;
								}
							});

					List<String> stateRow2 = thirdAllNamesToTimes.collect();
					if (stateRow2 == null || stateRow2.size() == 0) {
					} else {
						for (String row : stateRow2) {
							String[] rowArr = TAB.split(row);
							hm.put(rowArr[0], Long.valueOf(rowArr[1]));
						}
					}

				}
			}
		}
	}

	public static void fourALLDimensions(SQLContext sqlContext) {
		DataFrame fourALLDimensionToTimes = sqlContext
				.sql("select count(*) from PlayFailLog ");
		fourALLDimensionToTimes.show();
		JavaRDD<String> fourAllNamesToTimes = fourALLDimensionToTimes
				.toJavaRDD().map(new Function<Row, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public String call(Row row) {
						String time = DateUtil.getCurrentDateTime("yyyyMMddHHmm");
						String kk = "ALL#ALL#ALL#ALL#" + time + "\t" + row.getLong(0);
						return kk;
					}
				});

		List<String> stateRow = fourAllNamesToTimes.collect();
		if (stateRow == null || stateRow.size() == 0) {
		} else {
			for (String row : stateRow) {
				String[] rowArr = TAB.split(row);
				hm.put(rowArr[0], Long.valueOf(rowArr[1]));
			}
		}
	}

	static class JavaSQLContextSingleton {
		private transient static SQLContext instance = null;

		public static SQLContext getInstance(SparkContext sparkContext) {
			if (instance == null) {
				instance = new SQLContext(sparkContext);
			}
			return instance;
		}
	}

}
