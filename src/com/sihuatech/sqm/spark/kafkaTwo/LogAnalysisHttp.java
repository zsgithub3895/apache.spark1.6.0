package com.sihuatech.sqm.spark.kafkaTwo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Created by yuechao on 2016/12/28.
 */
public class LogAnalysisHttp {
	public static void main(String[] args) {
		try {
			File logDir = new File("E:\\ra_11-13");
			if (!logDir.isDirectory())
				System.exit(1);

			Map<String, Long> maxMap = new HashMap<String, Long>();

			int count = 0;
			long sum = 0;

			for (File logFile : logDir.listFiles()) {
				 //System.out.println("开始处理日志文件" + logFile.getName());
				BufferedReader reader = new BufferedReader(new FileReader(logFile));
				String log;
				while ((log = reader.readLine()) != null) {
					String[] fields = log.split(String.valueOf((char) 0x7F));
					if ("3".equals(fields[0]) && "23".equals(fields[7]) && "369".equals(fields[8])) {
						//2016-08-01 12:14:53
						fields[23] = fields[23].replace("-", "").replace(":", "").replace(" ", "");
						if("20170110120000".compareTo(fields[2]) <= 0 && "20170110130000".compareTo(fields[2]) > 0 ){
						//if("20170110130000".compareTo(fields[23]) <= 0 && "20170110140000".compareTo(fields[23]) > 0 ){
							count++;
							String hasId = fields[1];
							Long downBytes = Long.valueOf(fields[17]);
							if (maxMap.containsKey(hasId)) {
								long value = maxMap.get(hasId);
								if (value < downBytes) {
									maxMap.put(hasId, downBytes);
								}
							} else {
								maxMap.put(hasId, downBytes);
							}
							//System.out.println(fields[2]+","+fields[23]);
						}
					}
				}
			}
			System.out.println("广西桂林市的日志条数：" + count);
			System.out.println("maxMap 尺寸=" + maxMap.size());

			for (Entry<String, Long> en : maxMap.entrySet()) {
				//System.out.println(en.getKey());

				long max = en.getValue();
				sum += max;
			}
			System.out.println("广西桂林市的HTTP总流量 sum=" + sum + ",换算单位G=" + (sum / 1024 / 1024 / 1024));
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
