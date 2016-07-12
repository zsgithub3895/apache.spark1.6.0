package com.sihuatech.sqm.spark;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
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

import com.sihuatech.sqm.spark.bean.EPGResponseBean;
import com.sihuatech.sqm.spark.util.DateUtil;
import com.sihuatech.sqm.spark.util.PropHelper;

import scala.Tuple2;

/**
 * EPG响应指标分析-平均响应时长，单位微秒
 * @author chuql 2016年6月29日
 */
public final class EPGResponseAnalysis {
	private static Logger logger = Logger.getLogger(EPGResponseAnalysis.class);
	private static final long DEFAULT_AVG = 0;
	private static final Pattern TAB = Pattern.compile("\t");
	private static final String KEY_PREFIX = "EPG_RSP";
	//平均响应时长单位微秒
	private static Map<String,Long> epgAvgMap = new HashMap<String,Long>();

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		if (args.length < 4) {
			System.err.println("Usage: EPGResponseAnalysis <zkQuorum> <group> <topics> <numThreads>");
			System.exit(1);
		}
		
		String epgResponseTime = PropHelper.getProperty("EPG_RESPONSE_TIME");
		if(null != epgResponseTime && !"".equals(epgResponseTime)){
			logger.info(String.format("EPG响应指标执行参数 : \n zkQuorum : %s \n group : %s \n topic : %s \n numThread : %s \n duration : %s",args[0],
					args[1],args[2],args[3],epgResponseTime));
			SparkConf conf = new SparkConf().setAppName("EPGResponseAnalysis");
			JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(Integer.valueOf(epgResponseTime)));
			
			//此参数为接收Topic的线程数，并非Spark分析的分区数
			int numThreads = Integer.parseInt(args[3]);
			Map<String, Integer> topicMap = new HashMap<String, Integer>();
			String[] topics = args[2].split(",");
			for (String topic : topics) {
				topicMap.put(topic, numThreads);
			}
			JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, args[0], args[1],
					topicMap);

			JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
				@Override
				public String call(Tuple2<String, String> tuple2) {
					return tuple2._2();
				}
			});
			// 校验日志，过滤不符合条件的记录
			JavaDStream<String> filterLines = lines.filter(new Function<String,Boolean>(){
				@Override
				public Boolean call(String line) throws Exception {
					String[] lineArr = line.split("\\|",-1);
					if(lineArr.length < 12){
						return false;
					}else if(!"6".equals(lineArr[0])){
						return false;
					}else if("".equals(lineArr[2])){
						return false;
					}else if("".equals(lineArr[3])){
						return false;
					}else if("".equals(lineArr[6])){
						return false;
					}else if("".equals(lineArr[8])){
						return false;
					}
					return true;
				}
			});
			
			filterLines.foreachRDD(new Function2<JavaRDD<String>, Time, Void>() {
				@Override
				public Void call(JavaRDD<String> rdd, Time time) {
					String[] columns = {"provinceID","platform","pageType","epgDur"};
					SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());

					JavaRDD<EPGResponseBean> rowRDD = rdd.map(new Function<String, EPGResponseBean>() {
						public EPGResponseBean call(String line) {
							String[] lineArr = line.split("\\|",-1);
							EPGResponseBean record = new EPGResponseBean();
							record.setPlatform(lineArr[2].trim());
							record.setProvinceID(lineArr[3].trim());
							record.setPageType(lineArr[6].trim());
							record.setEpgDur(Long.valueOf(lineArr[8].trim()));
							return record;
						}
					});
					//注册临时表
					DataFrame epgDataFrame = sqlContext.createDataFrame(rowRDD, EPGResponseBean.class);
					epgDataFrame.registerTempTable("epgresponse");
					
					long total = epgDataFrame.count();//计算单位时间段内记录总数
					if(total > 0){
						//处理非ALL维度的数据
						noAllByDimension(sqlContext, columns);
						
						//处理ALL维度的数据
						allByDimension(sqlContext, columns);
					}
					
                    //保存分析数据
					if(epgAvgMap.size() > 0){
						save();
						//清空本次分析数据
						epgAvgMap.clear();
					}
					return null;
				}
			});
			jssc.start();
			jssc.awaitTermination();
		}else{
			logger.info("请配置每隔多长时间取播放请求日志！！");
		}
	}
	
	private static void save(){
		logger.info("保存分析数据到存储系统开始");
		String time = DateUtil.getCurrentDateTime("yyyyMMddHHmm");
		logger.info("保存到Redis");
		/* 图片加载分析指标及时率写入Redis */
		IndexToRedis.toRedisOnLong(epgAvgMap, KEY_PREFIX+"#", "#"+time);
		logger.info("保存到Mysql");
		/* 图片加载分析指标及时率写入MySQL */
		IndexToMysql.toMysqlOnEPGIndex(epgAvgMap, time);
		logger.info("保存分析数据到存储系统结束");
	}
	
	/**
	 * 处理非ALL维度的数据
	 * 分析计算某省份某牌照方某种页面类型的EPG平均响应时长
	 */
	private static void noAllByDimension(SQLContext sqlContext,String[] columns){
		DataFrame avgFrame = sqlContext.sql("select "+columns[0]+","+columns[1]+","+columns[2]+",avg("+columns[3]+") as avg from epgresponse group by "+columns[0]+","+columns[1]+","+columns[2]);
		JavaRDD<String> avgRDD = avgFrame.toJavaRDD().map(new Function<Row, String>() {
			@Override
			public String call(Row row) throws Exception {
				return row.getString(0)+"#"+row.getString(1)+"#"+row.getString(2)+"\t"+row.getDouble(3);
			}
		});
		
		List<String> avgtRow = avgRDD.collect();
		if(avgtRow == null || avgtRow.size() == 0){
			return;
		}else{
			for(String  row: avgtRow){
				String[] rowArr = TAB.split(row);
				epgAvgMap.put(rowArr[0], Math.round(Double.valueOf(rowArr[1])));
			}
		}
	}
	
	
	/**
	 * 处理ALL维度的数据
	 * 分析计算全国某牌照方某种页面类型的EPG平均响应时长和某省份所有牌照方某种页面类型的EPG平均响应时长
	 */
	private static void allByDimension(SQLContext sqlContext,String[] columns){
		//全国某牌照方某种页面类型的EPG平均响应时长
		processByPlatform(sqlContext, columns);
		
    	//某省份所有牌照方某种页面类型的EPG平均响应时长
		processByProvince(sqlContext, columns);
		
		//全国所有牌照方某种页面类型的EPG平均响应时长
		process(sqlContext, columns);
	}
	/**
	 * 分析计算全国某牌照方的图片加载及时率
	 */
	private static void processByPlatform(SQLContext sqlContext,String[] columns){
		DataFrame avgFrame = sqlContext.sql("select "+columns[1]+","+columns[2]+",avg("+columns[3]+") as avg from epgresponse group by "+columns[1]+","+columns[2]);
		JavaRDD<String> avgRDD = avgFrame.toJavaRDD().map(new Function<Row, String>() {
			@Override
			public String call(Row row) throws Exception {
				return "ALL#"+row.getString(0)+"#"+row.getString(1)+"\t"+row.getDouble(2);
			}
		});
		List<String> avgtRow = avgRDD.collect();
		if(avgtRow == null || avgtRow.size() == 0){
			return;
		}else{
			for(String  row: avgtRow){
				String[] rowArr = TAB.split(row);
				epgAvgMap.put(rowArr[0], Math.round(Double.valueOf(rowArr[1])));
			}
		}
	}
	/**
	 * 分析计算某省份所有牌照方的图片加载及时率
	 */
	private static void processByProvince(SQLContext sqlContext,String[] columns){
		DataFrame avgFrame = sqlContext.sql("select "+columns[0]+","+columns[2]+",avg("+columns[3]+") as avg from epgresponse group by "+columns[0]+","+columns[2]);
		JavaRDD<String> avgRDD = avgFrame.toJavaRDD().map(new Function<Row, String>() {
			@Override
			public String call(Row row) throws Exception {
				return row.getString(0)+"#ALL#"+row.getString(1)+"\t"+row.getDouble(2);
			}
		});
		List<String> avgtRow = avgRDD.collect();
		if(avgtRow == null || avgtRow.size() == 0){
			return;
		}else{
			for(String  row: avgtRow){
				String[] rowArr = TAB.split(row);
				epgAvgMap.put(rowArr[0], Math.round(Double.valueOf(rowArr[1])));
			}
		}
	}
	/**
	 * 分析计算全国所有牌照方的图片加载及时率
	 */
	private static void process(SQLContext sqlContext,String[] columns){
		DataFrame avgFrame = sqlContext.sql("select "+columns[2]+",avg("+columns[3]+") as avg from epgresponse group by "+columns[2]);
		JavaRDD<String> avgRDD = avgFrame.toJavaRDD().map(new Function<Row, String>() {
			@Override
			public String call(Row row) throws Exception {
				return "ALL#ALL#"+row.getString(0)+"\t"+row.getDouble(1);
			}
		});
		List<String> avgtRow = avgRDD.collect();
		if(avgtRow == null || avgtRow.size() == 0){
			return;
		}else{
			for(String  row: avgtRow){
				String[] rowArr = TAB.split(row);
				epgAvgMap.put(rowArr[0], Math.round(Double.valueOf(rowArr[1])));
			}
		}
	}
}
