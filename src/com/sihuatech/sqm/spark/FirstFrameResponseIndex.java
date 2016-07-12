package com.sihuatech.sqm.spark;

import java.io.Serializable;
import java.util.HashMap;
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
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.sihuatech.sqm.spark.bean.PlayResponseLog;
import com.sihuatech.sqm.spark.commen.Constant;
import com.sihuatech.sqm.spark.util.DateUtil;
import com.sihuatech.sqm.spark.util.PropHelper;

import scala.Tuple2;

public class FirstFrameResponseIndex {
	private static Logger logger = Logger.getLogger(FirstFrameResponseIndex.class);
	private static HashMap<String, Double> hm = new HashMap<String, Double>();

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		if (null == args || args.length < 4) {
			System.err.println("Usage: FirstFrameResponseIndex <zkQuorum> <group> <topics> <numThreads>");
			System.exit(1);
		}
		SparkConf sparkConf = new SparkConf().setAppName("FirstFrameResponseIndex");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		String FIRST_FRAME_TIME = PropHelper.getProperty("FIRST_FRAME_TIME");
		// 日志以|线分隔
		final Broadcast<Pattern> V_LINE = ctx.broadcast(Pattern.compile("\\|"));
		if (FIRST_FRAME_TIME != null && !FIRST_FRAME_TIME.equals("")) {
			JavaStreamingContext jssc = new JavaStreamingContext(ctx,
					Durations.minutes(Long.valueOf(FIRST_FRAME_TIME)));

			//此参数为接收Topic的线程数，并非Spark分析的分区数
			int numThreads = Integer.parseInt(args[3]);
			Map<String, Integer> topicMap = new HashMap<String, Integer>();
			logger.info("开始读取内容：");
			String[] topics = args[2].split(",");
			for (String topic : topics) {
				logger.info("topic：" + topic);
				topicMap.put(topic, numThreads);
			}
			JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, args[0], args[1],
					topicMap);

			JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
				private static final long serialVersionUID = 1L;

				@Override
				public String call(Tuple2<String, String> tuple2) {
					logger.info("++++++++++[首帧响应指标日志]" + tuple2._2);
					return tuple2._2();
				}
			});
			
			lines.foreachRDD(new Function2<JavaRDD<String>, Time, Void>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Void call(JavaRDD<String> rdd, Time time) {
					String[] dimension = {"provinceID", "platform","deviceProvider", "fwVersion"};
					SQLContext sqlContext = JavaSQLContextSingleton.getInstance(rdd.context());
					// Convert JavaRDD[String] to JavaRDD[bean class] to
					JavaRDD<PlayResponseLog> rowRDD = rdd.map(new Function<String, PlayResponseLog>() {
						private static final long serialVersionUID = 1L;

						public PlayResponseLog call(String word) {
							PlayResponseLog playResponseLog = null;
							if (StringUtils.isNotBlank(word)) {
								String[] fields = V_LINE.value().split(word);
								try {
									if(fields.length>16){
									   if (null != fields[16] || !fields[16].equals("")) {
										   playResponseLog = new PlayResponseLog(fields[5], fields[6],fields[7], fields[9], Long.valueOf(fields[16]));
										  }
									}
								} catch (ArrayIndexOutOfBoundsException e) {
									logger.error("=====================首帧响应指标日志中latency字段为空，舍弃", e);
								}
							}
							return playResponseLog;
						}
					});

					// 校验日志，过滤不符合条件的记录
					JavaRDD<PlayResponseLog> playResponseLogs = rowRDD.filter(new Function<PlayResponseLog, Boolean>() {
						@Override
						public Boolean call(PlayResponseLog play) throws Exception {
							if (null==play) {
								return false;
							}
							return true;
						}
					});
					DataFrame wordsDataFrame = sqlContext.createDataFrame(playResponseLogs, PlayResponseLog.class);
					wordsDataFrame.registerTempTable("FlayResponseLog");

					/**维度不包含ALL *********************************************************************/
					noALLByDimension(sqlContext, dimension);

					/**维度中包含ALL ******************************************************************/
					differentALLByDimension(sqlContext, dimension);
					return null;
				}
			});
			jssc.start();
			jssc.awaitTermination();
		} else {
			logger.info("请配置每隔多长时间取播放请求日志！！");
		}
	}

	public static void noALLByDimension(SQLContext sqlContext, String[] dimension) {
		DataFrame noALLDimension = sqlContext.sql("select avg(latency)," + dimension[0] + "," + dimension[1]
				+ "," + dimension[2] + "," + dimension[3] + " from FlayResponseLog group by " + dimension[0] + ","
				+ dimension[1] + "," + dimension[2] + "," + dimension[3]);
            noALLDimension.toJavaRDD().map(new Function<Row, Void>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Void call(Row row) {
					String time = DateUtil.getCurrentDateTime("yyyyMMddHHmm");
					String kk = Constant.FIRST_FRAME + "#" + row.getString(1) + "#" + row.getString(2) + "#"
							+ row.getString(3) + "#" + row.getString(4) + "#" + time;
					logger.info("维度中不包含ALL的key：" + kk + "    首帧响应指标：" + row.getDouble(0));
					hm.put(kk,row.getDouble(0));
				return null;
			}
		}).collect();

	}

	public static void differentALLByDimension(SQLContext sqlContext, String[] dimension) {
		StringBuffer sb = null;
		/**四个维度为ALL*/
		fourALLDimensions(sqlContext, sb);
	
		/**三个维度为ALL*/
		for (int i = 0; i < dimension.length; i++) {
			sb = new StringBuffer();
			sb.append("select avg(latency)," + i + "," + dimension[i] + " from FlayResponseLog group by "
					+ dimension[i]);
			DataFrame dAll = sqlContext.sql(sb.toString());
			dAll.show();
			 dAll.toJavaRDD().map(new Function<Row, Void>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Void call(Row row) {
					String time = DateUtil.getCurrentDateTime("yyyyMMddHHmm");
					String kk = null;
					if (row.getInt(1) == 0) {
						kk = Constant.FIRST_FRAME + "#" + row.getString(2) + "#ALL#ALL#ALL#" + time;
					}
					if (row.getInt(1) == 1) {
						kk = Constant.FIRST_FRAME + "#ALL#" + row.getString(2) + "#ALL#ALL#" + time;
					}
					if (row.getInt(1) == 2) {
						kk = Constant.FIRST_FRAME + "#ALL#ALL#" + row.getString(2) + "#ALL#" + time;
					}
					if (row.getInt(1) == 3) {
						kk = Constant.FIRST_FRAME + "#ALL#ALL#ALL#" + row.getString(2) + "#" + time;
					}
					logger.info("三个维度为ALL的key：" + kk + "   首帧响应指标：" + row.getDouble(0));
					hm.put(kk, row.getDouble(0));
					return null;
				}
			}).collect();

			logger.info("三个维度为ALL的sql语句----" + sb.toString());
			
			/**两个维度为ALL*/
			for (int j = i + 1; j < dimension.length; j++) {
				sb = new StringBuffer();
				sb.append("select avg(latency)," + i + j + "," + dimension[i] + "," + dimension[j]
						+ " from FlayResponseLog group by " + dimension[i] + "," + dimension[j]);

				DataFrame ddAll = sqlContext.sql(sb.toString());
				ddAll.show();
				ddAll.toJavaRDD().map(new Function<Row, Void>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Void call(Row row) {
						String time = DateUtil.getCurrentDateTime("yyyyMMddHHmm");
						String kk = null;
						if (row.getInt(1) == 1) {
							kk = Constant.FIRST_FRAME + "#" + row.getString(2) + "#" + row.getString(3) + "#ALL#ALL#"
									+ time;
						}
						if (row.getInt(1) == 2) {
							kk = Constant.FIRST_FRAME + "#" + row.getString(2) + "#ALL#" + row.getString(3) + "#ALL#"
									+ time;
						}
						if (row.getInt(1) == 3) {
							kk = Constant.FIRST_FRAME + "#" + row.getString(2) + "#ALL#ALL#" + row.getString(3) + "#"
									+ time;
						}
						if (row.getInt(1) == 12) {
							kk = Constant.FIRST_FRAME + "#ALL#" + row.getString(2) + "#" + row.getString(3) + "#ALL#"
									+ time;
						}
						if (row.getInt(1) == 13) {
							kk = Constant.FIRST_FRAME + "#ALL#" + row.getString(2) + "#ALL#" + row.getString(3) + "#"
									+ time;
						}
						if (row.getInt(1) == 23) {
							kk = Constant.FIRST_FRAME + "#ALL#ALL#" + row.getString(2) + "#" + row.getString(3) + "#"
									+ time;
						}
						logger.info("两个维度为ALL的key：" + kk + "   首帧响应指标：" + row.getDouble(0));
						hm.put(kk, row.getDouble(0));
						return null;
					}
				}).collect();

				logger.info("两个维度为ALL的sql语句--" + sb.toString());

				/**一个维度为ALL*/
				for (int k = j + 1; k < dimension.length; k++) {
					sb = new StringBuffer();
					sb.append("select avg(latency)," + i + j + k + "," + dimension[i] + "," + dimension[j]
							+ "," + dimension[k] + " from FlayResponseLog group by " + dimension[i] + "," + dimension[j]
							+ "," + dimension[k]);
					DataFrame dddAll = sqlContext.sql(sb.toString());
					dddAll.show();
					dddAll.toJavaRDD().map(new Function<Row, Void>() {
						private static final long serialVersionUID = 1L;
						@Override
						public Void call(Row row) {
							String time = DateUtil.getCurrentDateTime("yyyyMMddHHmm");
							String kk = null;
							if (row.getInt(1) == 12) {
								kk = Constant.FIRST_FRAME + "#" + row.getString(2) + "#" + row.getString(3) + "#"
										+ row.getString(4) + "#ALL#" + time;
							}
							if (row.getInt(1) == 13) {
								kk = Constant.FIRST_FRAME + "#" + row.getString(2) + "#" + row.getString(3) + "#ALL#"
										+ row.getString(4) + "#" + time;
							}
							if (row.getInt(1) == 23) {
								kk = Constant.FIRST_FRAME + "#" + row.getString(2) + "#ALL#" + row.getString(3) + "#"
										+ row.getString(4) + "#" + time;
							}
							if (row.getInt(1) == 123) {
								kk = Constant.FIRST_FRAME + "#ALL#" + row.getString(2) + "#" + row.getString(3) + "#"
										+ row.getString(4) + "#" + time;
							}
							logger.info("一个维度为ALL的key:" + kk + "   首帧响应指标：" + row.getDouble(0));
							hm.put(kk, row.getDouble(0));
							return null;
						}
					}).collect();

					logger.info("一个维度为ALL的sql语句--" + sb.toString());
				}
			}
		}

		if (hm != null && hm.size() > 0) {
			/** 首帧响应指标入Redis **/
			IndexToRedis.toRedis(hm);
			/** 首帧响应指标入mysql **/
			IndexToMysql.toMysql(hm);
			hm.clear();
		}

	}

	public static void fourALLDimensions(SQLContext sqlContext,StringBuffer sb){
		sb =new StringBuffer();
		sb.append("select avg(latency),count(*) from FlayResponseLog ");
		DataFrame fourALLDimension = sqlContext.sql(sb.toString());
		fourALLDimension.show();
		fourALLDimension.toJavaRDD().map(new Function<Row, Void>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Void call(Row row) {
				if(row.getLong(1)!=0){
					String time = DateUtil.getCurrentDateTime("yyyyMMddHHmm");
					String kk = Constant.FIRST_FRAME + "#ALL#ALL#ALL#ALL#" + time;
					logger.info("四个维度为ALL的key：" + kk + "   首帧响应指标：" + row.getDouble(0));
					hm.put(kk, row.getDouble(0));
				}
				return null;
			}
		}).collect();
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

	public static class PlayResponse implements Serializable {
		private static final long serialVersionUID = 1L;
		private double latency=0.0;
		private long count=0l;
		private String key;

		public double getLatency() {
			return latency;
		}

		public void setLatency(double latency) {
			this.latency = latency;
		}

		public long getCount() {
			return count;
		}

		public void setCount(long count) {
			this.count = count;
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}
	}

}