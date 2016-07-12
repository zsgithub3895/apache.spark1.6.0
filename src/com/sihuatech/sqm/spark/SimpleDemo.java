package com.sihuatech.sqm.spark;


import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.sihuatech.sqm.spark.bean.LagPhaseBehaviorLog;


public class SimpleDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("simpledemo").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlCtx = new SQLContext(sc);
        testUDF(sc, sqlCtx);
        sc.stop();
        sc.close();
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
}