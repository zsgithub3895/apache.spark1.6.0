package com.sihuatech.sqm.spark;

import java.io.IOException;
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

import com.sihuatech.sqm.spark.bean.TerminalInfo;
import com.sihuatech.sqm.spark.bean.TerminalState;
import com.sihuatech.sqm.spark.commen.Constant;
import com.sihuatech.sqm.spark.redis.RedisClient;
import com.sihuatech.sqm.spark.util.DateUtil;
import com.sihuatech.sqm.spark.util.PropHelper;

public class TerminalStatePlayCount {

	private static Logger logger = Logger.getLogger(TerminalStatePlayCount.class);
	private static HashMap<String, Long> hm = new HashMap<String, Long>();
	private static final Pattern TAB = Pattern.compile("\t");
	private static RedisClient client;

	public static void setClient(RedisClient client) {
		TerminalStatePlayCount.client = client;
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception{
		if (null == args || args.length < 4) {
			System.err.println("Usage: TerminalStatePlayCount <zkQuorum> <group> <topics> <numThreads>");
			System.exit(1);
		}
		SparkConf sparkConf = new SparkConf().setAppName("TerminalStatePlayCount");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		String TERMINAL_STATE_TIME = PropHelper.getProperty("TERMINAL_STATE_TIME");
		
		if (TERMINAL_STATE_TIME != null && !TERMINAL_STATE_TIME.equals("")) {
			JavaStreamingContext jssc = new JavaStreamingContext(ctx,
					Durations.seconds(Long.valueOf(TERMINAL_STATE_TIME)));
			
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
					logger.info("++++++++-----------++[机顶盒状态播放次数日志]" + tuple2._2);
					return tuple2._2();
				}
			});
			
			// 校验日志，过滤不符合条件的记录
			JavaDStream<String> filterLines = lines.filter(new Function<String, Boolean>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Boolean call(String line) throws Exception {
					String[] lineArr = line.split("\\|", -1);
					if (lineArr.length < 7) {
						return false;
					} else if (!"2".equals(lineArr[0])) {
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
					}
						return true;
				}
			});
			
			filterLines.foreachRDD(new Function2<JavaRDD<String>, Time, Void>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Void call(JavaRDD<String> rdd, Time time) {
					String[] dimension = { "provinceID", "platform", "deviceProvider", "fwVersion" };
					SQLContext sqlContext = JavaSQLContextSingleton.getInstance(rdd.context());
					// Convert JavaRDD[String] to JavaRDD[bean class] to
					JavaRDD<TerminalState> rowRDD = rdd.map(new Function<String, TerminalState>() {
						private static final long serialVersionUID = 1L;

						public TerminalState call(String word) throws Exception {
							TerminalState terminalState = null;
							if (StringUtils.isNotBlank(word)) {
								String[] fields = word.split("\\|", -1);
								try {
									if (client == null) {
										RedisClient client = new RedisClient();
										client.init();
										setClient(client);
										logger.info("#############################");
									}
									// 根据ProbeID查询Redis的terminalInfo
									//RedisClient client = new RedisClient();
									TerminalInfo info = null;
									try {
										info = (TerminalInfo) client.getObject(fields[1]);
									} catch (IOException e) {
										logger.error("从redis获取数据失败", e);
									}
									if(info != null){
										terminalState = new TerminalState();
										terminalState.setDeviceProvider(info.getDeviceProvider());
										terminalState.setPlatform(info.getPlatform());
										terminalState.setProvinceID(info.getProvinceID());
										terminalState.setFwVersion(info.getFwVersion());
										terminalState.setProbeID(fields[1]);
										terminalState.setHasID(fields[2]);
									}else{
										logger.info("------------------+null");
									}
								} catch (ArrayIndexOutOfBoundsException e) {
									logger.error("=====================机顶盒状态日志中字段为空，舍弃", e);
								}
							}
							return terminalState;
						}
					});

					DataFrame wordsDataFrame = sqlContext.createDataFrame(rowRDD, TerminalState.class);
					wordsDataFrame.registerTempTable("TerminalState");

					/**维度不包含ALL *********************************************************************/
					noALLByDimension(sqlContext, dimension);

					/**维度中包含ALL ******************************************************************/
					differentALLByDimension(sqlContext, dimension);
					
					save();
					return null;
				}
			});
			jssc.start();
			jssc.awaitTermination();
		}
	}
	
	public static void save(){
		if (hm != null && hm.size() > 0) {
			String time = DateUtil.getCurrentDateTime("yyyyMMddHHmm");
			IndexToRedis.toRedisToLong(hm, time);
//			IndexToMysql.toMysql(hm);
			hm.clear();
		}
	}
	
	public static void noALLByDimension(SQLContext sqlContext, String[] dimension) {
		DataFrame noALLDimensionToTimes = sqlContext.sql("select count(DISTINCT hasID)," + dimension[0] + "," + dimension[1]
				+ "," + dimension[2] + "," + dimension[3] + " from TerminalState group by " + dimension[0] + ","
				+ dimension[1] + "," + dimension[2] + "," + dimension[3]);
//		DataFrame noALLDimensionToCount = sqlContext.sql("select count(DISTINCT probeID)," + dimension[0] + "," + dimension[1]
//				+ "," + dimension[2] + "," + dimension[3] + " from TerminalState group by " + dimension[0] + ","
//				+ dimension[1] + "," + dimension[2] + "," + dimension[3]);
		JavaRDD<String> noALLNamesToTimes = noALLDimensionToTimes.toJavaRDD().map(new Function<Row, String>() {

			@Override
			public String call(Row row) {
				String kk = Constant.ALL_PLAY + "#" + row.getString(1) + "#" + row.getString(2) + "#"
						+ row.getString(3) + "#" + row.getString(4)+"\t"+ row.getLong(0);
				return kk;
			}
		});
		
//		List<STBState> noALLNamesToCount = noALLDimensionToCount.toJavaRDD().map(new Function<Row, STBState>() {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public STBState call(Row row) {
//				STBState p = new STBState();
//				p.setUserCount(row.getLong(0));
//				String time = DateUtil.getCurrentDateTime("yyyyMMddHHmm");
//				String kk = Constant.ALL_USER + "#" + row.getString(1) + "#" + row.getString(2) + "#"
//						+ row.getString(3) + "#" + row.getString(4) + "#" + time;
//				p.setUserCountKey(kk);
//				return p;
//			}
//		}).collect();

		List<String> stateRow = noALLNamesToTimes.collect();
		if (stateRow == null || stateRow.size()==0) {
		}else{
			for(String  row: stateRow){
				String[] rowArr = TAB.split(row);
				hm.put(rowArr[0], Long.valueOf(rowArr[1]));
			}
		}
		
//		if (CollectionUtils.isNotEmpty(noALLNamesToTimes)) {
//			for (STBState pp : noALLNamesToTimes) {
//				if (pp.getPlayTime() != 0) {
//					logger.info("维度中不包含ALL的key：" + pp.getPlayTimeKey() + "    播放总次数：" + pp.getPlayTime());
//					hm.put(pp.getPlayTimeKey(), pp.getPlayTime());
//				}
//			}
//		}
//		if (CollectionUtils.isNotEmpty(noALLNamesToCount)) {
//			for (STBState pp : noALLNamesToCount) {
//				if (pp.getUserCount() != 0) {
//					logger.info("维度中不包含ALL的key：" + pp.getUserCountKey() + "    播放总用户数：" + pp.getUserCount());
//					hm.put(pp.getUserCountKey(), pp.getUserCount());
//				}
//			}
//		}
	}
	
	public static void differentALLByDimension(SQLContext sqlContext, String[] dimension) {
		/**四个维度为ALL*/
		fourALLDimensions(sqlContext);
	
		/**三个维度为ALL*/
		for (int i = 0; i < dimension.length; i++) {
			StringBuffer sb = new StringBuffer();
			sb.append("select count(DISTINCT hasID)," + i + "," + dimension[i] + " from TerminalState group by "
					+ dimension[i]);
			DataFrame dAllToTimes = sqlContext.sql(sb.toString());
			dAllToTimes.show();
			JavaRDD<String> oneAllNamesToTimes = dAllToTimes.toJavaRDD().map(new Function<Row, String>() {

				@Override
				public String call(Row row) {
					String kk = null;
					if (row.getInt(1) == 0) {
						kk = Constant.ALL_PLAY + "#" + row.getString(2) + "#ALL#ALL#ALL" +"\t"+ row.getLong(0);
					}
					if (row.getInt(1) == 1) {
						kk = Constant.ALL_PLAY + "#ALL#" + row.getString(2) + "#ALL#ALL" +"\t"+ row.getLong(0);
					}
					if (row.getInt(1) == 2) {
						kk = Constant.ALL_PLAY + "#ALL#ALL#" + row.getString(2) + "#ALL" +"\t"+ row.getLong(0);
					}
					if (row.getInt(1) == 3) {
						kk = Constant.ALL_PLAY + "#ALL#ALL#ALL#" + row.getString(2)  +"\t"+ row.getLong(0);
					}
					return kk;
				}
			});
			
			List<String> stateRow = oneAllNamesToTimes.collect();
			if (stateRow == null || stateRow.size()==0) {
			}else{
				for(String  row: stateRow){
					String[] rowArr = TAB.split(row);
					hm.put(rowArr[0], Long.valueOf(rowArr[1]));
				}
			}

//			if (oneAllNamesToTimes != null && oneAllNamesToTimes.size() > 0) {
//				for (STBState pp : oneAllNamesToTimes) {
//					if (pp.getPlayTime() != 0) {
//						logger.info("三个维度为ALL的key：" + pp.getPlayTimeKey() + "   播放总次数：" + pp.getPlayTime());
//					hm.put(pp.getPlayTimeKey(), pp.getPlayTime());
//					}
//					
//				}
//			}
			
//			StringBuffer sb1 = new StringBuffer();
//			sb1.append("select count(DISTINCT probeID)," + i + "," + dimension[i] + " from TerminalState group by "
//					+ dimension[i]);
//			DataFrame dAllToCount = sqlContext.sql(sb1.toString());
//			dAllToCount.show();
//			List<STBState> oneAllNamesToCount = dAllToCount.toJavaRDD().map(new Function<Row, STBState>() {
//				private static final long serialVersionUID = 1L;
//
//				@Override
//				public STBState call(Row row) {
//					STBState p = new STBState();
//					p.setUserCount(row.getLong(0));
//					String time = DateUtil.getCurrentDateTime("yyyyMMddHHmm");
//					String kk = null;
//					if (row.getInt(1) == 0) {
//						kk = Constant.ALL_USER + "#" + row.getString(2) + "#ALL#ALL#ALL#" + time;
//					}
//					if (row.getInt(1) == 1) {
//						kk = Constant.ALL_USER + "#ALL#" + row.getString(2) + "#ALL#ALL#" + time;
//					}
//					if (row.getInt(1) == 2) {
//						kk = Constant.ALL_USER + "#ALL#ALL#" + row.getString(2) + "#ALL#" + time;
//					}
//					if (row.getInt(1) == 3) {
//						kk = Constant.ALL_USER + "#ALL#ALL#ALL#" + row.getString(2) + "#" + time;
//					}
//					p.setUserCountKey(kk);;
//					return p;
//				}
//			}).collect();
//
//			if (oneAllNamesToCount != null && oneAllNamesToCount.size() > 0) {
//				for (STBState pp : oneAllNamesToCount) {
//					if (pp.getUserCount() != 0) {
//						logger.info("三个维度为ALL的key：" + pp.getUserCountKey() + "   播放总用户数：" + pp.getUserCount());
//					hm.put(pp.getUserCountKey(), pp.getUserCount());
//
//					}
//					
//				}
//			}
//			
//			logger.info("三个维度为ALL的播放总次数sql语句----" + sb.toString());
//			logger.info("三个维度为ALL的播放总用户数sql语句----" + sb1.toString());
			
			/**两个维度为ALL*/
			for (int j = i + 1; j < dimension.length; j++) {
				sb = new StringBuffer();
				sb.append("select count(DISTINCT hasID)," + i + j + "," + dimension[i] + "," + dimension[j]
						+ " from TerminalState group by " + dimension[i] + "," + dimension[j]);

				DataFrame ddAllToTimes = sqlContext.sql(sb.toString());
				ddAllToTimes.show();
				JavaRDD<String> twoAllNamesToTimes = ddAllToTimes.toJavaRDD().map(new Function<Row, String>() {

					@Override
					public String call(Row row) {
						String kk = null;
						if (row.getInt(1) == 1) {
							kk = Constant.ALL_PLAY + "#" + row.getString(2) + "#" + row.getString(3) + "#ALL#ALL"
									+"\t"+ row.getLong(0);
						}
						if (row.getInt(1) == 2) {
							kk = Constant.ALL_PLAY + "#" + row.getString(2) + "#ALL#" + row.getString(3) + "#ALL"
									+"\t"+ row.getLong(0);
						}
						if (row.getInt(1) == 3) {
							kk = Constant.ALL_PLAY + "#" + row.getString(2) + "#ALL#ALL#" + row.getString(3) 
									+"\t"+ row.getLong(0);
						}
						if (row.getInt(1) == 12) {
							kk = Constant.ALL_PLAY + "#ALL#" + row.getString(2) + "#" + row.getString(3) + "#ALL"
									+"\t"+ row.getLong(0);
						}
						if (row.getInt(1) == 13) {
							kk = Constant.ALL_PLAY + "#ALL#" + row.getString(2) + "#ALL#" + row.getString(3) 
									+"\t"+ row.getLong(0);
						}
						if (row.getInt(1) == 23) {
							kk = Constant.ALL_PLAY + "#ALL#ALL#" + row.getString(2) + "#" + row.getString(3) 
									+"\t"+ row.getLong(0);
						}
						return kk;
					}
				});

				List<String> stateRow1 = twoAllNamesToTimes.collect();
				if (stateRow1 == null || stateRow1.size()==0) {
				}else{
					for(String  row: stateRow1){
						String[] rowArr = TAB.split(row);
						hm.put(rowArr[0], Long.valueOf(rowArr[1]));
					}
				}
				
//				if (twoAllNamesToTimes != null && twoAllNamesToTimes.size() > 0) {
//					for (STBState pp : twoAllNamesToTimes) {
//						if (pp.getPlayTime() != 0) {
//							logger.info("两个维度为ALL的key：" + pp.getPlayTimeKey() + "   播放总次数：" + pp.getPlayTime());
//							hm.put(pp.getPlayTimeKey(), pp.getPlayTime());
//						}
//					}
//				}
				
				
//				sb1 = new StringBuffer();
//				sb1.append("select count(DISTINCT probeID)," + i + j + "," + dimension[i] + "," + dimension[j]
//						+ " from TerminalState group by " + dimension[i] + "," + dimension[j]);
//
//				DataFrame ddAllToCount = sqlContext.sql(sb1.toString());
//				ddAllToCount.show();
//				List<STBState> twoAllNamesToCount = ddAllToCount.toJavaRDD().map(new Function<Row, STBState>() {
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public STBState call(Row row) {
//						STBState p = new STBState();
//						p.setUserCount(row.getLong(0));
//						String time = DateUtil.getCurrentDateTime("yyyyMMddHHmm");
//						String kk = null;
//						if (row.getInt(1) == 1) {
//							kk = Constant.ALL_USER + "#" + row.getString(2) + "#" + row.getString(3) + "#ALL#ALL#"
//									+ time;
//						}
//						if (row.getInt(1) == 2) {
//							kk = Constant.ALL_USER + "#" + row.getString(2) + "#ALL#" + row.getString(3) + "#ALL#"
//									+ time;
//						}
//						if (row.getInt(1) == 3) {
//							kk = Constant.ALL_USER + "#" + row.getString(2) + "#ALL#ALL#" + row.getString(3) + "#"
//									+ time;
//						}
//						if (row.getInt(1) == 12) {
//							kk = Constant.ALL_USER + "#ALL#" + row.getString(2) + "#" + row.getString(3) + "#ALL#"
//									+ time;
//						}
//						if (row.getInt(1) == 13) {
//							kk = Constant.ALL_USER + "#ALL#" + row.getString(2) + "#ALL#" + row.getString(3) + "#"
//									+ time;
//						}
//						if (row.getInt(1) == 23) {
//							kk = Constant.ALL_USER + "#ALL#ALL#" + row.getString(2) + "#" + row.getString(3) + "#"
//									+ time;
//						}
//						p.setUserCountKey(kk);
//						return p;
//					}
//				}).collect();
//
//				if (twoAllNamesToCount != null && twoAllNamesToCount.size() > 0) {
//					for (STBState pp : twoAllNamesToCount) {
//						if (pp.getUserCount() != 0) {
//							logger.info("两个维度为ALL的key：" + pp.getUserCountKey() + "   播放总用户数：" + pp.getUserCount());
//							hm.put(pp.getUserCountKey(), pp.getUserCount());
//						}
//					}
//				}
//				logger.info("两个维度为ALL的播放总次数sql语句--" + sb.toString());
//				logger.info("两个维度为ALL的播放总用户数sql语句--" + sb1.toString());

				/**一个维度为ALL*/
				for (int k = j + 1; k < dimension.length; k++) {
					sb = new StringBuffer();
					sb.append("select count(DISTINCT hasID)," + i + j + k + "," + dimension[i] + "," + dimension[j]
							+ "," + dimension[k] + " from TerminalState group by " + dimension[i] + "," + dimension[j]
							+ "," + dimension[k]);
					DataFrame dddAllToTimes = sqlContext.sql(sb.toString());
					dddAllToTimes.show();
					JavaRDD<String> thirdAllNamesToTimes = dddAllToTimes.toJavaRDD().map(new Function<Row, String>() {

						@Override
						public String call(Row row) {
							String kk = null;
							if (row.getInt(1) == 12) {
								kk = Constant.ALL_PLAY + "#" + row.getString(2) + "#" + row.getString(3) + "#"
										+ row.getString(4) + "#ALL" +"\t"+ row.getLong(0);
							}
							if (row.getInt(1) == 13) {
								kk = Constant.ALL_PLAY + "#" + row.getString(2) + "#" + row.getString(3) + "#ALL#"
										+ row.getString(4)  +"\t"+ row.getLong(0);
							}
							if (row.getInt(1) == 23) {
								kk = Constant.ALL_PLAY + "#" + row.getString(2) + "#ALL#" + row.getString(3) + "#"
										+ row.getString(4)  +"\t"+ row.getLong(0);
							}
							if (row.getInt(1) == 123) {
								kk = Constant.ALL_PLAY + "#ALL#" + row.getString(2) + "#" + row.getString(3) + "#"
										+ row.getString(4)  +"\t"+ row.getLong(0);
							}
							return kk;
						}
					});

					List<String> stateRow2 = thirdAllNamesToTimes.collect();
					if (stateRow2 == null || stateRow2.size()==0) {
					}else{
						for(String  row: stateRow2){
							String[] rowArr = TAB.split(row);
							hm.put(rowArr[0], Long.valueOf(rowArr[1]));
						}
					}
					
//					if (thirdAllNamesToTimes != null && thirdAllNamesToTimes.size() > 0) {
//						for (STBState pp : thirdAllNamesToTimes) {
//							if (pp.getPlayTime() != 0) {
//								logger.info("一个维度为ALL的key：" + pp.getPlayTimeKey() + "   播放总次数：" + pp.getPlayTime());
//								hm.put(pp.getPlayTimeKey(), pp.getPlayTime());
//							}
//						}
//					}
					
					
//					sb1 = new StringBuffer();
//					sb1.append("select count(DISTINCT probeID)," + i + j + k + "," + dimension[i] + "," + dimension[j]
//							+ "," + dimension[k] + " from TerminalState group by " + dimension[i] + "," + dimension[j]
//							+ "," + dimension[k]);
//					DataFrame dddAllToCount = sqlContext.sql(sb1.toString());
//					dddAllToCount.show();
//					List<STBState> thirdAllNamesToCount = dddAllToCount.toJavaRDD().map(new Function<Row, STBState>() {
//						private static final long serialVersionUID = 1L;
//
//						@Override
//						public STBState call(Row row) {
//							STBState p = new STBState();
//							p.setUserCount(row.getLong(0));
//							String time = DateUtil.getCurrentDateTime("yyyyMMddHHmm");
//							String kk = null;
//							if (row.getInt(1) == 12) {
//								kk = Constant.ALL_USER + "#" + row.getString(2) + "#" + row.getString(3) + "#"
//										+ row.getString(4) + "#ALL#" + time;
//							}
//							if (row.getInt(1) == 13) {
//								kk = Constant.ALL_USER + "#" + row.getString(2) + "#" + row.getString(3) + "#ALL#"
//										+ row.getString(4) + "#" + time;
//							}
//							if (row.getInt(1) == 23) {
//								kk = Constant.ALL_USER + "#" + row.getString(2) + "#ALL#" + row.getString(3) + "#"
//										+ row.getString(4) + "#" + time;
//							}
//							if (row.getInt(1) == 123) {
//								kk = Constant.ALL_USER + "#ALL#" + row.getString(2) + "#" + row.getString(3) + "#"
//										+ row.getString(4) + "#" + time;
//							}
//							p.setUserCountKey(kk);
//							return p;
//						}
//					}).collect();
//
//					if (thirdAllNamesToCount != null && thirdAllNamesToCount.size() > 0) {
//						for (STBState pp : thirdAllNamesToCount) {
//							if (pp.getUserCount() != 0) {
//								logger.info("一个维度为ALL的key：" + pp.getUserCountKey() + "   播放总用户数：" + pp.getUserCount());
//								hm.put(pp.getUserCountKey(), pp.getUserCount());
//							}
//						}
//					}
//					logger.info("一个维度为ALL的播放总次数sql语句--" + sb.toString());
//					logger.info("一个维度为ALL的播放总用户数sql语句--" + sb1.toString());
				}
			}
		}
	}
	
	
	public static void fourALLDimensions(SQLContext sqlContext){
		DataFrame fourALLDimensionToTimes = sqlContext.sql("select count(DISTINCT hasID) from TerminalState ");
		fourALLDimensionToTimes.show();
		JavaRDD<String> fourAllNamesToTimes = fourALLDimensionToTimes.toJavaRDD().map(new Function<Row, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Row row) {
				String kk = Constant.ALL_PLAY + "#ALL#ALL#ALL#ALL" +"\t"+ row.getLong(0);
				return kk;
			}
		});

		List<String> stateRow = fourAllNamesToTimes.collect();
		if (stateRow == null || stateRow.size()==0) {
		}else{
			for(String  row: stateRow){
				String[] rowArr = TAB.split(row);
				hm.put(rowArr[0], Long.valueOf(rowArr[1]));
			}
		}
		
//		if (CollectionUtils.isNotEmpty(fourAllNamesToTimes)) {
//			for (STBState pp : fourAllNamesToTimes) {
//				if (pp.getPlayTime() != 0) {
//					logger.info("四个维度为ALL的key：" + pp.getPlayTimeKey() + "    播放总次数：" + pp.getPlayTime());
//					hm.put(pp.getPlayTimeKey(), pp.getPlayTime());
//				}
//			}
//		}
		
		
//		DataFrame fourALLDimensionToCount = sqlContext.sql("select count(DISTINCT probeID) from TerminalState ");
//		fourALLDimensionToCount.show();
//		List<STBState> fourAllNamesToCount = fourALLDimensionToCount.toJavaRDD().map(new Function<Row, STBState>() {
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public STBState call(Row row) {
//				STBState p = new STBState();
//				p.setUserCount(row.getLong(0));
//				String time = DateUtil.getCurrentDateTime("yyyyMMddHHmm");
//				String kk = Constant.ALL_PLAY + "#ALL#ALL#ALL#ALL#" + time;
//				p.setUserCountKey(kk);;
//				return p;
//			}
//		}).collect();
//
//		if (CollectionUtils.isNotEmpty(fourAllNamesToCount)) {
//			for (STBState pp : fourAllNamesToCount) {
//				if (pp.getUserCount() != 0) {
//					logger.info("四个维度为ALL的key：" + pp.getUserCountKey() + "    播放总用户数：" + pp.getUserCount());
//					hm.put(pp.getUserCountKey(), pp.getUserCount());
//				}
//			}
//		}
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
	
//	public static class STBState implements Serializable {
//		private static final long serialVersionUID = 1L;
//		private long playTime=0;
//		private long userCount=0;
//		private String playTimeKey;
//		private String userCountKey;
//		public long getPlayTime() {
//			return playTime;
//		}
//		public void setPlayTime(long playTime) {
//			this.playTime = playTime;
//		}
//		public long getUserCount() {
//			return userCount;
//		}
//		public void setUserCount(long userCount) {
//			this.userCount = userCount;
//		}
//		public String getPlayTimeKey() {
//			return playTimeKey;
//		}
//		public void setPlayTimeKey(String playTimeKey) {
//			this.playTimeKey = playTimeKey;
//		}
//		public String getUserCountKey() {
//			return userCountKey;
//		}
//		public void setUserCountKey(String userCountKey) {
//			this.userCountKey = userCountKey;
//		}
//	}
}
