package com.sihuatech.sqm.spark;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
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

import com.sihuatech.sqm.spark.FirstFrameResponseIndex.JavaSQLContextSingleton;
import com.sihuatech.sqm.spark.bean.LagPhaseBehaviorLog;
import com.sihuatech.sqm.spark.commen.Constant;
import com.sihuatech.sqm.spark.util.DateUtil;
import com.sihuatech.sqm.spark.util.PropHelper;

import scala.Tuple2;

public class LagphasePlayUser {
	private static Logger logger = Logger.getLogger(LagphasePlayUser.class);
	private static HashMap<String, Long> userMap=new HashMap<String,Long>();

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		if (null == args || args.length < 4) {
			System.err.println("Usage: LagphasePlayUser <zkQuorum> <group> <topics> <numThreads>");
			System.exit(1);
		}
		SparkConf sparkConf = new SparkConf().setAppName("LagphasePlayUser");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		String lagphasePlayUser = PropHelper.getProperty("LAG_PHASE_PLAY_USER");
		logger.info("+++++++++++++++++++++++++"+lagphasePlayUser);
		// 日志以|线分隔
		final Broadcast<Pattern> V_LINE = ctx.broadcast(Pattern.compile("\\|"));
		final Broadcast<HashMap<String, Long>> userMapB = ctx.broadcast(userMap);
		if (null != lagphasePlayUser && !lagphasePlayUser.equals("")) {
			JavaStreamingContext jssc = new JavaStreamingContext(ctx,
					Durations.minutes(Long.valueOf(lagphasePlayUser)));

			// 此参数为接收Topic的线程数，并非Spark分析的分区数
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
					logger.info("++++++++++[卡顿行为日志]" + tuple2._2);
					return tuple2._2();
				}
			});

			lines.foreachRDD(new Function2<JavaRDD<String>, Time, Void>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Void call(JavaRDD<String> rdd, Time time) {
					String[] dimension = { "provinceID", "platform", "deviceProvider", "fwVersion"};
					SQLContext sqlContext = JavaSQLContextSingleton.getInstance(rdd.context());
					JavaRDD<LagPhaseBehaviorLog> rowRDD = rdd.map(new Function<String, LagPhaseBehaviorLog>() {
						private static final long serialVersionUID = 1L;

						public LagPhaseBehaviorLog call(String word) {
							LagPhaseBehaviorLog lagLog = null;
							if (StringUtils.isNotBlank(word)) {
								String[] lineArr = V_LINE.value().split(word);
								try {
									lagLog = new LagPhaseBehaviorLog(Integer.valueOf(lineArr[0]),lineArr[2], lineArr[3], lineArr[4], lineArr[5],
											lineArr[7]);
								} catch (ArrayIndexOutOfBoundsException e) {
									logger.error("=====================", e);
								}
							}
							return lagLog;
						}
					});

					DataFrame lagDataFrame = sqlContext.createDataFrame(rowRDD, LagPhaseBehaviorLog.class);
					lagDataFrame.registerTempTable("LagPhaseLog");

					//** 维度不包含ALL *********************************************************************//*
					noALLByDimension(sqlContext, dimension,userMapB);
					//** 维度中包含ALL ******************************************************************//*
					differentALLByDimension(sqlContext, dimension,userMapB);
					return null;
				}
			});
			jssc.start();
			jssc.awaitTermination();
		} else {
			logger.info("请配置每隔多长时间取卡顿行为日志！！");
		}
	}
	
	 public static void noALLByDimension(SQLContext sqlContext, String[] dimension,final Broadcast<HashMap<String, Long>> hhm) {
			DataFrame noALLDimension = sqlContext.sql("select count(distinct probeID)," + dimension[0] + "," + dimension[1]
					+ "," + dimension[2] + "," + dimension[3] + " from LagPhaseLog group by " + dimension[0] + ","
					+ dimension[1] + "," + dimension[2] + "," + dimension[3]);
			noALLDimension.show();
			noALLDimension.toJavaRDD().map(new Function<Row, Void>() {
				private static final long serialVersionUID = 1L;
				@Override
				public Void call(Row row) {
					String time = DateUtil.getCurrentDateTime("yyyyMMddHHmm");
					String kk = Constant.LAG_USER +"#"+ row.getString(1) + "#" + row.getString(2) + "#"
							+ row.getString(3) + "#" + row.getString(4) + "#" + time;
					logger.info("维度中不包含ALL的key：" + kk + "    卡顿播放用户数：" + row.getLong(0));
					hhm.getValue().put(kk, row.getLong(0));
					return null;
				}
			}).collect();
			
		}
	 
	 public static void differentALLByDimension(SQLContext sqlContext, String[] dimension,final Broadcast<HashMap<String, Long>> hhm) {
			StringBuffer sb = null;
			/**四个维度为ALL*/
			fourALLDimensions(sqlContext, sb,hhm);
		
			/**三个维度为ALL*/
			for (int i = 0; i < dimension.length; i++) {
				sb = new StringBuffer();
				sb.append("select count(distinct probeID)," + i + "," + dimension[i] + " from LagPhaseLog group by "
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
							kk = Constant.LAG_USER + "#" + row.getString(2) + "#ALL#ALL#ALL#" + time;
						}
						if (row.getInt(1) == 1) {
							kk = Constant.LAG_USER + "#ALL#" + row.getString(2) + "#ALL#ALL#" + time;
						}
						if (row.getInt(1) == 2) {
							kk = Constant.LAG_USER + "#ALL#ALL#" + row.getString(2) + "#ALL#" + time;
						}
						if (row.getInt(1) == 3) {
							kk = Constant.LAG_USER + "#ALL#ALL#ALL#" + row.getString(2) + "#" + time;
						}
						logger.info("维度中包含三个ALL的key：" + kk + "    卡顿播放用户数：" + row.getLong(0));
						hhm.getValue().put(kk, row.getLong(0));
						return null;
					}
				}).collect();

				logger.info("三个维度为ALL的sql语句----" + sb.toString());
				
				/**两个维度为ALL*/
				for (int j = i + 1; j < dimension.length; j++) {
					sb = new StringBuffer();
					sb.append("select count(distinct probeID)," + i + j + "," + dimension[i] + "," + dimension[j]
							+ " from LagPhaseLog group by " + dimension[i] + "," + dimension[j]);

					DataFrame ddAll = sqlContext.sql(sb.toString());
					ddAll.show();
				    ddAll.toJavaRDD().map(new Function<Row, Void>() {
						private static final long serialVersionUID = 1L;
						@Override
						public Void call(Row row) {
							String time = DateUtil.getCurrentDateTime("yyyyMMddHHmm");
							String kk = null;
							if (row.getInt(1) == 1) {
								kk = Constant.LAG_USER + "#" + row.getString(2) + "#" + row.getString(3) + "#ALL#ALL#"
										+ time;
							}
							if (row.getInt(1) == 2) {
								kk = Constant.LAG_USER + "#" + row.getString(2) + "#ALL#" + row.getString(3) + "#ALL#"
										+ time;
							}
							if (row.getInt(1) == 3) {
								kk = Constant.LAG_USER + "#" + row.getString(2) + "#ALL#ALL#" + row.getString(3) + "#"
										+ time;
							}
							if (row.getInt(1) == 12) {
								kk = Constant.LAG_USER + "#ALL#" + row.getString(2) + "#" + row.getString(3) + "#ALL#"
										+ time;
							}
							if (row.getInt(1) == 13) {
								kk = Constant.LAG_USER + "#ALL#" + row.getString(2) + "#ALL#" + row.getString(3) + "#"
										+ time;
							}
							if (row.getInt(1) == 23) {
								kk = Constant.LAG_USER + "#ALL#ALL#" + row.getString(2) + "#" + row.getString(3) + "#"
										+ time;
							}
							logger.info("维度中包含两个维度为ALL的key：" + kk + "    卡顿播放用户数：" + row.getLong(0));
							hhm.getValue().put(kk, row.getLong(0));
							return null;
						}
					}).collect();

					logger.info("两个维度为ALL的sql语句--" + sb.toString());

					/**一个维度为ALL*/
					for (int k = j + 1; k < dimension.length; k++) {
						sb = new StringBuffer();
						sb.append("select count(distinct probeID)," + i + j + k + "," + dimension[i] + "," + dimension[j]
								+ "," + dimension[k] + " from LagPhaseLog group by " + dimension[i] + "," + dimension[j]
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
									kk = Constant.LAG_USER + "#" + row.getString(2) + "#" + row.getString(3) + "#"
											+ row.getString(4) + "#ALL#" + time;
								}
								if (row.getInt(1) == 13) {
									kk = Constant.LAG_USER + "#" + row.getString(2) + "#" + row.getString(3) + "#ALL#"
											+ row.getString(4) + "#" + time;
								}
								if (row.getInt(1) == 23) {
									kk = Constant.LAG_USER + "#" + row.getString(2) + "#ALL#" + row.getString(3) + "#"
											+ row.getString(4) + "#" + time;
								}
								if (row.getInt(1) == 123) {
									kk = Constant.LAG_USER + "#ALL#" + row.getString(2) + "#" + row.getString(3) + "#"
											+ row.getString(4) + "#" + time;
								}
								logger.info("维度中包含一个维度为ALL的key：" + kk + "    卡顿播放用户数：" + row.getLong(0));
								hhm.getValue().put(kk, row.getLong(0));
								return null;
							}
						}).collect();

						logger.info("一个维度为ALL的sql语句--" + sb.toString());
					}
				}
			}

			if (null!= hhm.getValue() && hhm.getValue().size() > 0) {
				logger.info("++++++++++++++++++++++++++++playuser中map的大小=" + hhm.getValue().size());
				/** 卡顿播放用户数入Redis,mysql **/
				IndexToRedis.playUserToRedis(hhm.getValue());
				hhm.getValue().clear();
			}

		}
	 
		public static void fourALLDimensions(SQLContext sqlContext,StringBuffer sb,final Broadcast<HashMap<String, Long>> hhm){
			sb =new StringBuffer();
			sb.append("select count(distinct probeID) from LagPhaseLog ");
			DataFrame fourALLDimension = sqlContext.sql(sb.toString());
			fourALLDimension.show();
		    fourALLDimension.toJavaRDD().map(new Function<Row, Void>() {
				private static final long serialVersionUID = 1L;
				@Override
				public Void call(Row row) {
						String time = DateUtil.getCurrentDateTime("yyyyMMddHHmm");
						String kk = Constant.LAG_USER + "#ALL#ALL#ALL#ALL#" + time;
						logger.info("维度中包含所有ALL的key：" + kk + "    卡顿播放用户数：" + row.getLong(0));
						hhm.getValue().put(kk, row.getLong(0));
						return null;
				}
			}).collect();
		}
}

