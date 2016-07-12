package com.sihuatech.sqm.spark;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.sihuatech.sqm.spark.commen.Constant;
import com.sihuatech.sqm.spark.redis.RedisClient;
import com.sihuatech.sqm.spark.util.PropHelper;

public class IndexToRedis {
	private static Logger logger = Logger.getLogger(IndexToRedis.class);
	static DecimalFormat df = new DecimalFormat("0.0000");
	private static final int POLLING_TIMES = 3; // 默认轮询3次

	public static void toRedis(HashMap<String, Double> hm) {
		RedisClient client = new RedisClient();
		try {
			client.init();
			for (Entry<String, Double> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					client.setObject(en.getKey(), Double.parseDouble(df.format(en.getValue())));
				}
			}
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}

	public static void toRedisToLong(HashMap<String, Long> hm) {
		RedisClient client = new RedisClient();
		try {
			client.init();
			for (Entry<String, Long> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					client.setObject(en.getKey(), en.getValue());
				}
			}
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}

	public static void toRedis(Map<String, Double> hm, String prefix, String suffix) {
		RedisClient client = new RedisClient();
		try {
			client.init();
			for (Entry<String, Double> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					client.setObject(prefix + en.getKey() + suffix, en.getValue());
					logger.info("redis key:"+prefix + en.getKey() + suffix+",value:"+en.getValue());
				}
			}
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}
	public static void toRedisOnLong(Map<String, Long> hm, String prefix, String suffix) {
		RedisClient client = new RedisClient();
		try {
			client.init();
			for (Entry<String, Long> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					client.setObject(prefix + en.getKey() + suffix, en.getValue());
					logger.info("redis key:"+prefix + en.getKey() + suffix+",value:"+en.getValue());
				}
			}
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}

	public static void playCountToRedis(HashMap<String, Long> hm) {
		RedisClient client = new RedisClient();
		HashMap<String, Double> mapMysql = new HashMap<String, Double>();
		try {
			client.init();
			for (Entry<String, Long> en : hm.entrySet()) {
				if (null != en.getKey() && null != en.getValue()) {
					double r = 0d;
					String delPrefix = en.getKey().substring(en.getKey().indexOf("#"));
					String newAllKey = Constant.ALL_PLAY + delPrefix;
					logger.info("数据newAllKey1=" + newAllKey);
					if (null != client.getObject(newAllKey)) {
						int value = (Integer) client.getObject(newAllKey);
						if (value != 0) {
							r = Double.parseDouble(df.format((double) en.getValue() / value));
							client.setObject(en.getKey(), r);
							mapMysql.put(en.getKey(), r);
						}
					} else {
						try {
							Calendar fromDate = Calendar.getInstance();
							SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
							Date to = sdf.parse(en.getKey().split("\\#", -1)[5]);
							String lagphasePlayCount = PropHelper.getProperty("LAG_PHASE_PLAY_COUNT");
							int count = Integer.valueOf(lagphasePlayCount);
							logger.info("redis中不存在数据newAllKey1=" + newAllKey+",根据配置时间向前轮询3次");
							// 根据配置时间向前轮询3次
							int result = 0;
							for (int i = 0; i < POLLING_TIMES; i++) {
								fromDate.setTime(to);
								String oldTime=sdf.format(fromDate.getTime());
								fromDate.add(Calendar.MINUTE, -count);
								String newTime=sdf.format(fromDate.getTime());
								newAllKey=newAllKey.replaceAll(oldTime, newTime);
								logger.info("数据newAllKey2=" + newAllKey);
								try {
									result = (Integer) client.getObject(newAllKey);
								} catch (NullPointerException e) {
									logger.info("redis中不存在key=" + newAllKey + "的数据");
								}
								if (result == 0) {
									logger.info("没有[" + newAllKey + "]的数据，往前推" + count * (i + 1) + "分钟再查");
									to=sdf.parse(newTime);
								} else {
									break;
								}
							}
							if (result != 0) {
								logger.info("入redis中的key=" + en.getKey());
								r = Double.parseDouble(df.format((double) en.getValue() / result));
								client.setObject(en.getKey(), r);
								mapMysql.put(en.getKey(), r);
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}

				}
			}
			IndexToMysql.playCountToMysql(mapMysql);
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}

	public static void playUserToRedis(HashMap<String, Long> hm) {
		RedisClient client = new RedisClient();
		HashMap<String, Double> mapMysql = new HashMap<String, Double>();
		try {
			client.init();
			for (Entry<String, Long> en : hm.entrySet()) {
				if (null != en.getKey() && null != en.getValue()) {
					double r = 0d;
					String delPrefix = en.getKey().substring(en.getKey().indexOf("#"));
					String newAllKey = Constant.ALL_USER + delPrefix;
					if (null != client.getObject(newAllKey)) {
						int value = (Integer) client.getObject(newAllKey);
						if (value != 0) {
							r = Double.parseDouble(df.format((double) en.getValue() / value));
							client.setObject(en.getKey(), r);
							mapMysql.put(en.getKey(), r);
						}
					} else {
						try {
							Calendar fromDate = Calendar.getInstance();
							SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");
							Date to = sdf.parse(en.getKey().split("\\#", -1)[5]);
							String lagphasePlayUser = PropHelper.getProperty("LAG_PHASE_PLAY_USER");
							int count = Integer.valueOf(lagphasePlayUser);
							logger.info("redis中不存在数据newAllKey1=" + newAllKey+",根据配置时间向前轮询3次");
							// 根据配置时间向前轮询3次
							long result = 0;
							for (int i = 0; i < POLLING_TIMES; i++) {
								fromDate.setTime(to);
								String oldTime=sdf.format(fromDate.getTime());
								fromDate.add(Calendar.MINUTE, -count);
								String newTime=sdf.format(fromDate.getTime());
								newAllKey=newAllKey.replaceAll(oldTime, newTime);
								try {
									result = (Integer) client.getObject(newAllKey);
								} catch (NullPointerException e) {
									logger.info("redis中不存在key=" + newAllKey + "的数据");
								}
								if (result == 0) {
									logger.info("没有[" + newAllKey + "]的数据，往前推" + count * (i + 1) + "分钟再查");
								    to=sdf.parse(newTime);
								} else {
									break;
								}
							}
							if (result != 0) {
								logger.info("入redis中的key=" + en.getKey());
								r = Double.parseDouble(df.format((double) en.getValue() / result));
								client.setObject(en.getKey(), r);
								mapMysql.put(en.getKey(), r);
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}

				}
			}
			IndexToMysql.playUserCountToMysql(mapMysql);
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}
	
	public static void toRedisToLong(HashMap<String, Long> hm, String time) {
		RedisClient client = new RedisClient();
		try {
			client.init();
			for (Entry<String, Long> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					client.setObject(en.getKey()+"#"+time, en.getValue());
				}
			}
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}
	
	public static void playSuccToRedis(Map<String, Long> hm){
		RedisClient client = new RedisClient();
		HashMap<String, Double> map = new HashMap<String, Double>();
		
		Calendar fromDate = Calendar.getInstance();
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmm");
		try {
			client.init();
			for (Entry<String, Long> en : hm.entrySet()) {
				if (null != en.getKey() && null != en.getValue()) {
					double r = 0d;
					String allKey = Constant.ALL_PLAY + "#" +en.getKey();
					if (null != client.getObject(allKey)) {
						long value = (Long) client.getObject(allKey);
						if (value != 0) {
							long playSuccCount = value - en.getValue();
							logger.info("播放成功次数:"+playSuccCount+",总次数："+value);
							r = Double.parseDouble(df.format((double) playSuccCount / value));
							String succKey = Constant.PLAY_SUCC+"#"+en.getKey();
							client.setObject(succKey, r);
							map.put(en.getKey(), r);
						}
					}else {
						try {
							String PLAY_SUCC_TIME = PropHelper.getProperty("LAG_PHASE_PLAY_USER");
							if (null != PLAY_SUCC_TIME) {
								int time = Integer.valueOf(PLAY_SUCC_TIME);
								Date to = df.parse(en.getKey().split("\\#", -1)[4]);
								// 根据配置时间向前轮询3次
								long result = 0;
								for (int i = 0; i < POLLING_TIMES; i++) {
									fromDate.setTime(to);
									String oldTime=df.format(fromDate.getTime());
									fromDate.add(Calendar.MINUTE, -time);
									String newTime=df.format(fromDate.getTime());
									allKey=allKey.replaceAll(oldTime, newTime);
										try {
											result = (Integer) client.getObject(allKey);
										} catch (NullPointerException e) {
											logger.info("redis中不存在key=" + allKey + "的数据");
										}
										if (result == 0) {
											logger.info("没有[" + allKey + "]的数据，往前推" + time * (i + 1) + "秒再查");
											to=df.parse(newTime);
										} else {
											break;
										}
									}
								if (result != 0) {
									long playSuccCount = result - en.getValue();
									logger.info("播放成功次数:"+playSuccCount+",总次数："+result);
									r = Double.parseDouble(df.format((double) playSuccCount / result));
									String succKey = Constant.PLAY_SUCC+"#"+en.getKey();
									client.setObject(succKey, r);
									map.put(en.getKey(), r);
								}
							}
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}
			logger.info("-------------map个数："+map.size());
			IndexToMysql.playSuccRateToMysql(map);
		} catch (Exception e) {
			logger.error("redis初始化失败!", e);
		}
	}
	
}
