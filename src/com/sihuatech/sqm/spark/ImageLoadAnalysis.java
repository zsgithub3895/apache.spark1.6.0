package com.sihuatech.sqm.spark;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

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

import com.sihuatech.sqm.spark.bean.ImageLoadBean;
import com.sihuatech.sqm.spark.util.DateUtil;
import com.sihuatech.sqm.spark.util.PropHelper;

import scala.Tuple2;

/**
 * 图片加载指标分析-及时率
 * 
 * @author chuql 2016年6月29日
 */
public final class ImageLoadAnalysis {
	private static Logger logger = Logger.getLogger(ImageLoadAnalysis.class);
	private static final long TIME_LENGTH = 3000000;
	//保留小数位数
	private static final String KEEP_DECIMAL_NUM_FOUR = "0.0000";
	private static final Pattern TAB = Pattern.compile("\t");
	private static final String KEY_PREFIX = "IMAGE_LOAD";
	private static Map<String,Long> temp =  new HashMap<String,Long>();
	private static Map<String,Double> fastRateMap = new HashMap<String,Double>();

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		if (args.length < 4) {
			System.err.println("Usage: ImageLoadAnalysis <zkQuorum> <group> <topics> <numThreads>");
			System.exit(1);
		}
		
		String imageLoadTime = PropHelper.getProperty("IMAGE_LOAD_TIME");
		if(null != imageLoadTime && !"".equals(imageLoadTime)){
			logger.info(String.format("图片加载指标执行参数 : \n zkQuorum : %s \n group : %s \n topic : %s \n numThread : %s \n duration : %s",args[0],
					args[1],args[2],args[3],imageLoadTime));
			SparkConf conf = new SparkConf().setAppName("ImageLoadAnalysis");
			JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(Integer.valueOf(imageLoadTime)));
			
			JavaSparkContext jsc = jssc.sparkContext();
			if( jsc == null){
				jsc = new JavaSparkContext(conf);
			}
			//广播变量
			final Broadcast<Long> BC_TIME_LENGTH = jsc.broadcast(TIME_LENGTH);
			
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
					if(lineArr.length < 10){
						return false;
					}else if(!"7".equals(lineArr[0])){
						return false;
					}else if("".equals(lineArr[2])){
						return false;
					}else if("".equals(lineArr[3])){
						return false;
					}else if("".equals(lineArr[7])){
						return false;
					}
					return true;
				}
			});
			
			filterLines.foreachRDD(new Function2<JavaRDD<String>, Time, Void>() {
				@Override
				public Void call(JavaRDD<String> rdd, Time time) {
					String[] columns = {"provinceID","platform","pictureDur"};
					SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());

					JavaRDD<ImageLoadBean> rowRDD = rdd.map(new Function<String, ImageLoadBean>() {
						public ImageLoadBean call(String line) {
							String[] lineArr = line.split("\\|",-1);
							ImageLoadBean record = new ImageLoadBean();
							record.setPlatform(lineArr[2].trim());
							record.setProvinceID(lineArr[3].trim());
							record.setPictureDur(Long.valueOf(lineArr[7].trim()));
							return record;
						}
					});
					//注册临时表
					DataFrame imageDataFrame = sqlContext.createDataFrame(rowRDD, ImageLoadBean.class);
					imageDataFrame.registerTempTable("imageload");
					
					long total = imageDataFrame.count();//计算单位时间段内记录总数
					if(total > 0){
						//处理非ALL维度的数据
						noAllByDimension(sqlContext, columns, BC_TIME_LENGTH);
						
						//处理ALL维度的数据
						allByDimension(sqlContext, columns, BC_TIME_LENGTH,total);
					}
					
                    //保存分析数据
					if(fastRateMap.size() > 0){
						save();
						//清空本次分析数据
						fastRateMap.clear();
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
		IndexToRedis.toRedis(fastRateMap, KEY_PREFIX+"#", "#"+time);
		logger.info("保存到Mysql");
		/* 图片加载分析指标及时率写入MySQL */
		IndexToMysql.toMysqlOnImageIntex(fastRateMap, time);
		logger.info("保存分析数据到存储系统结束");
	}
	
	/**
	 * 处理非ALL维度的数据
	 * 分析计算某省份某牌照方的图片加载及时率
	 */
	private static void noAllByDimension(SQLContext sqlContext,String[] columns,Broadcast<Long> BC_TIME_LENGTH){
		DataFrame fastFrame = sqlContext.sql("select "+columns[0]+","+columns[1]+",count(*) as cnt from imageload where "+columns[2]+"<= "+BC_TIME_LENGTH.value()+" group by "+columns[0]+","+columns[1]);
		JavaRDD<String> fastRDD = fastFrame.toJavaRDD().map(new Function<Row, String>() {
			@Override
			public String call(Row row) throws Exception {
				return row.getString(0)+"#"+row.getString(1)+"\t"+row.getLong(2);
			}
		});
		//图片加载及时标志
		boolean fastFlag = true;
		List<String> fastRow = fastRDD.collect();
		if(fastRow == null || fastRow.size() == 0){
		   	//所有图片加载都不及时或没有数据
			fastFlag = false;
		}else{
			for(String  row: fastRow){
				String[] rowArr = TAB.split(row);
				temp.put(rowArr[0], Long.valueOf(rowArr[1]));
			}
		}
		DataFrame totalFrame = sqlContext.sql("select "+columns[0]+","+columns[1]+",count(*) as total from imageload group by "+columns[0]+","+columns[1]);
		JavaRDD<String> totalRDD = totalFrame.toJavaRDD().map(new Function<Row, String>() {
			@Override
			public String call(Row row) throws Exception {
				return row.getString(0)+"#"+row.getString(1)+"\t"+row.getLong(2);
			}
		});
		List<String> totalRow = totalRDD.collect();
		if(totalRow == null || totalRow.size() == 0){
			//没有数据
			return;
		}
		//保留4位小数
		DecimalFormat df = new DecimalFormat(KEEP_DECIMAL_NUM_FOUR);
		for(String  row: totalRow){
			String[] rowArr = TAB.split(row);
			if(fastFlag){
				String key = rowArr[0];
				long total = Long.valueOf(rowArr[1]).longValue();
				if(temp.containsKey(key)){
					double rate =  (double)temp.get(key).longValue()/total;
					fastRateMap.put(rowArr[0], Double.valueOf(df.format(rate)));
				}else{
					fastRateMap.put(rowArr[0], Double.parseDouble(KEEP_DECIMAL_NUM_FOUR));
				}
			}else{
				fastRateMap.put(rowArr[0], Double.parseDouble(KEEP_DECIMAL_NUM_FOUR));
			}
		}
		//清空临时map
		temp.clear();
	}
	
	
	/**
	 * 处理ALL维度的数据
	 * 分析计算全国某牌照方的图片加载及时率和某省份所有牌照方的图片加载及时率
	 */
	private static void allByDimension(SQLContext sqlContext,String[] columns,Broadcast<Long> BC_TIME_LENGTH,long total){
		if(total == 0){
			return;
		}
		//全国某牌照方的图片加载及时率
		processByPlatform(sqlContext, columns, BC_TIME_LENGTH, total);
		
    	//某省份所有牌照方的图片加载及时率
		processByProvince(sqlContext, columns, BC_TIME_LENGTH, total);
		
		//全国所有牌照方的图片加载及时率
		process(sqlContext, columns, BC_TIME_LENGTH, total);
	}
	/**
	 * 分析计算全国某牌照方的图片加载及时率
	 * @param sqlContext
	 * @param columns
	 * @param BC_TIME_LENGTH
	 * @param total
	 */
	private static void processByPlatform(SQLContext sqlContext,String[] columns,Broadcast<Long> BC_TIME_LENGTH,long total){
		DataFrame fastFrame = sqlContext.sql("select "+columns[1]+",count(*) as cnt from imageload where "+columns[2]+"<= "+BC_TIME_LENGTH.value()+" group by "+columns[1]);
		JavaRDD<String> fastRDD = fastFrame.toJavaRDD().map(new Function<Row, String>() {
			@Override
			public String call(Row row) throws Exception {
				return "ALL#"+row.getString(0)+"\t"+row.getLong(1);
			}
		});
		//图片加载及时标志
		boolean fastFlag = true;
		List<String> fastRow = fastRDD.collect();
		if(fastRow == null || fastRow.size() == 0){
		   	//所有图片加载都不及时或没有数据
			fastFlag = false;
		}else{
			for(String  row: fastRow){
				String[] rowArr = TAB.split(row);
				temp.put(rowArr[0], Long.valueOf(rowArr[1]));
			}
		}
		
		DataFrame totalFrame = sqlContext.sql("select distinct "+columns[1]+" from imageload");
		JavaRDD<String> totalRDD = totalFrame.toJavaRDD().map(new Function<Row, String>() {
			@Override
			public String call(Row row) throws Exception {
				return "ALL#"+row.getString(0);
			}
		});
		//保留4位小数
		DecimalFormat df = new DecimalFormat(KEEP_DECIMAL_NUM_FOUR);
		List<String> totalRow = totalRDD.collect();
		for(String  row: totalRow){
			if(fastFlag){
				if(temp.containsKey(row)){
					double rate =  (double)(temp.get(row).longValue())/total;
					fastRateMap.put(row, Double.valueOf(df.format(rate)));
				}else{
					fastRateMap.put(row, Double.parseDouble(KEEP_DECIMAL_NUM_FOUR));
				}
			}else{
				fastRateMap.put(row, Double.parseDouble(KEEP_DECIMAL_NUM_FOUR));
			}
		}
		//清空临时map
		temp.clear();
	}
	/**
	 * 分析计算某省份所有牌照方的图片加载及时率
	 * @param sqlContext
	 * @param columns
	 * @param BC_TIME_LENGTH
	 * @param total
	 */
	private static void processByProvince(SQLContext sqlContext,String[] columns,Broadcast<Long> BC_TIME_LENGTH,long total){
		DataFrame fastFrame = sqlContext.sql("select "+columns[0]+",count(*) as cnt from imageload where "+columns[2]+"<= "+BC_TIME_LENGTH.value()+" group by "+columns[0]);
		JavaRDD<String> fastRDD = fastFrame.toJavaRDD().map(new Function<Row, String>() {
			@Override
			public String call(Row row) throws Exception {
				return row.getString(0)+"#ALL"+"\t"+row.getLong(1);
			}
		});
		//图片加载及时标志
		boolean fastFlag = true;
		List<String> fastRow = fastRDD.collect();
		if(fastRow == null || fastRow.size() == 0){
		   	//所有图片加载都不及时或没有数据
			fastFlag = false;
		}else{
			for(String  row: fastRow){
				String[] rowArr = TAB.split(row);
				temp.put(rowArr[0], Long.valueOf(rowArr[1]));
			}
		}
		
		DataFrame totalFrame = sqlContext.sql("select distinct "+columns[0]+" from imageload");
		JavaRDD<String> totalRDD = totalFrame.toJavaRDD().map(new Function<Row, String>() {
			@Override
			public String call(Row row) throws Exception {
				return row.getString(0)+"#ALL";
			}
		});
		//保留4位小数
		DecimalFormat df = new DecimalFormat(KEEP_DECIMAL_NUM_FOUR);
		List<String> totalRow = totalRDD.collect();
		for(String  row: totalRow){
			if(fastFlag){
				if(temp.containsKey(row)){
					double rate =  (double)temp.get(row).longValue()/total;
					fastRateMap.put(row, Double.valueOf(df.format(rate)));
				}else{
					fastRateMap.put(row, Double.parseDouble(KEEP_DECIMAL_NUM_FOUR));
				}
			}else{
				fastRateMap.put(row, Double.parseDouble(KEEP_DECIMAL_NUM_FOUR));
			}
		}
		//清空临时map
		temp.clear();
	}
	/**
	 * 分析计算全国所有牌照方的图片加载及时率
	 * @param sqlContext
	 * @param columns
	 * @param BC_TIME_LENGTH
	 * @param total
	 */
	private static void process(SQLContext sqlContext,String[] columns,Broadcast<Long> BC_TIME_LENGTH,long total){
		DataFrame fastFrame = sqlContext.sql("select count(*) as cnt from imageload where "+columns[2]+"<= "+BC_TIME_LENGTH.value());
		JavaRDD<String> fastRDD = fastFrame.toJavaRDD().map(new Function<Row, String>() {
			@Override
			public String call(Row row) throws Exception {
				return "ALL#ALL"+"\t"+row.getLong(0);
			}
		});
		//图片加载及时标志
		boolean fastFlag = true;
		List<String> fastRow = fastRDD.collect();
		if(fastRow == null || fastRow.size() == 0){
		   	//所有图片加载都不及时
			fastFlag = false;
		}else{
			for(String  row: fastRow){
				String[] rowArr = TAB.split(row);
				temp.put(rowArr[0], Long.valueOf(rowArr[1]));
			}
		}
		
		//保留4位小数
		DecimalFormat df = new DecimalFormat(KEEP_DECIMAL_NUM_FOUR);
		String row = "ALL#ALL";
		if(fastFlag){
			if(temp.containsKey(row)){
				double rate =  (double)temp.get(row).longValue()/total;
				fastRateMap.put(row, Double.valueOf(df.format(rate)));
			}else{
				fastRateMap.put(row, Double.parseDouble(KEEP_DECIMAL_NUM_FOUR));
			}
		}else{
			fastRateMap.put(row, Double.parseDouble(KEEP_DECIMAL_NUM_FOUR));
		}
		//清空临时map
		temp.clear();
	}
}
