package com.sihuatech.sqm.spark;


import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.sihuatech.sqm.spark.bean.LagPhaseBehaviorLog;


public class SimpleDemo {
    public static void main(String[] args) {
      /*  SparkConf conf = new SparkConf().setAppName("simpledemo").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlCtx = new SQLContext(sc);
        testUDF2(sc, sqlCtx);
        sc.stop();
        sc.close();*/
    	
    	/*String s = "12#22#33#44";
		System.out.println(s.substring(0,s.lastIndexOf("#")));*/
    		/*int i=3;
    		System.out.println(i--);
    		System.out.println(i);
    		System.out.println(--i);*/
    	
    		/*float f = 1.39e-1f;
    		double d = 1.39e-1d;
    		System.out.println(f);
    		System.out.println(d);*/
    	
    	/*int b1 = 5;
    	int bb = 10;
    	//System.out.println(Integer.toBinaryString(b1));
    	System.out.println(8>>1);
    	System.out.println(8<<1);*/
    	for(int i=0;i<10;i++){
    		if(i==5){
        		return;
        	}else{
        		System.out.println(i);
        	}
    	}
    	
    }


    //测试spark sql的自定义函数
    public static void testUDF(JavaSparkContext sc, SQLContext sqlCtx) {
        // Create a account and turn it into a Schema RDD
        List<LagPhaseBehaviorLog> accList = new ArrayList<LagPhaseBehaviorLog>();
        accList.add(new LagPhaseBehaviorLog("1","1","3","4","5"));
        accList.add(new LagPhaseBehaviorLog("2","1","3","4","6"));
        accList.add(new LagPhaseBehaviorLog("1","3","3","4","7"));
        JavaRDD<LagPhaseBehaviorLog> accRDD = sc.parallelize(accList);

        DataFrame schemaData = sqlCtx.createDataFrame(accRDD, LagPhaseBehaviorLog.class);

        schemaData.registerTempTable("acc");

        // 编写自定义函数UDF
//        sqlCtx.registerFunction("strlength", new UDF1<String, Integer>() {
//            @Override
//            public Integer call(String str) throws Exception {
//                return str.length();
//            }
//        }, DataType.IntegerType);

        // 数据查询
       // DataFrame results = sqlCtx.sql("SELECT count(distinct hasID),deviceProvider FROM acc group by deviceProvider");
        DataFrame results = sqlCtx.sql("SELECT avg(hasID),deviceProvider FROM acc group by deviceProvider");
        results.show();
    }
    
    //测试spark sql的自定义函数
    public static void testUDF2(JavaSparkContext sc, SQLContext sqlContext) {
    	String[] dimension = {"provinceID", "platform","deviceProvider", "fwVersion"};
    	int num = 0;
		while (num < Math.pow(2, dimension.length)) {
			StringBuffer groupSb = new StringBuffer();
			StringBuffer selectSb = new StringBuffer();
			String numStr = "";
			for (int i = 0; i < dimension.length; i++) {
				// 二进制从低位到高位取值
				int bit = (num >> i) & 1;
				numStr += bit;
				if (bit == 1) {
					selectSb.append(dimension[i]).append(",");
					groupSb.append(dimension[i]).append(",");
				} else {
					selectSb.append("'ALL'").append(",");
				}
			}
			String sql = null;
			if (StringUtils.isBlank(groupSb.toString())) {
				sql = "select avg(latency)," + selectSb.substring(0, selectSb.length()-1) + "  from PlayResponseLog ";
			}else {
				sql = "select avg(latency)," + selectSb.substring(0, selectSb.length()-1) + "  from PlayResponseLog group by " + groupSb.substring(0, groupSb.length() - 1);
			}
			System.out.println(num+"+++++"+sql);
			num++;
			
    }
}
}