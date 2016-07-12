package com.sihuatech.sqm.spark;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.sihuatech.sqm.spark.util.DBConnection;

public class IndexToMysql {
	static DecimalFormat df = new DecimalFormat("0.0000");

	public static void toMysql(HashMap<String, Double> hm) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder insertSql = new StringBuilder();
			insertSql
					.append("INSERT INTO T_FIRST_FRAME_INDEX(provinceID,platform,deviceProvider,fwVersion,parseTime,latency)"
							+ " VALUES(?,?,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, Double> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String[] sArr = en.getKey().split("\\#", -1);
					stmt.setString(1, sArr[1]);
					stmt.setString(2, sArr[2]);
					stmt.setString(3, sArr[3]);
					stmt.setString(4, sArr[4]);
					stmt.setString(5, sArr[5]);
					stmt.setDouble(6, Double.parseDouble(df.format(en.getValue())));
					stmt.addBatch();
					count++;
					if (count % 100 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
			}
			if (hm.entrySet() != null && hm.entrySet().size() > 0 && hm.entrySet().size() % 100 != 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
	}

	public static void playCountToMysql(HashMap<String, Double> hm) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder insertSql = new StringBuilder();
			insertSql
					.append("INSERT INTO T_LAG_PHASE_PLAYCOUNT(provinceID,platform,deviceProvider,fwVersion,parseTime,playCountRate)"
							+ " VALUES(?,?,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, Double> en : hm.entrySet()) {
				if (null != en && null != en.getKey()) {
					String[] sArr = en.getKey().split("\\#", -1);
					stmt.setString(1, sArr[1]);
					stmt.setString(2, sArr[2]);
					stmt.setString(3, sArr[3]);
					stmt.setString(4, sArr[4]);
					stmt.setString(5, sArr[5]);
					stmt.setDouble(6, en.getValue());
					stmt.addBatch();
					count++;
					if (count % 100 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
			}
			if (hm.entrySet() != null && hm.entrySet().size() > 0 && hm.entrySet().size() % 100 != 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
	}

	public static void playUserCountToMysql(HashMap<String, Double> hm) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder insertSql = new StringBuilder();
			insertSql
					.append("INSERT INTO T_LAG_PHASE_PLAYUSER(provinceID,platform,deviceProvider,fwVersion,parseTime,playUserRate)"
							+ " VALUES(?,?,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, Double> en : hm.entrySet()) {
				if (null != en && null != en.getKey()) {
					String[] sArr = en.getKey().split("\\#", -1);
					stmt.setString(1, sArr[1]);
					stmt.setString(2, sArr[2]);
					stmt.setString(3, sArr[3]);
					stmt.setString(4, sArr[4]);
					stmt.setString(5, sArr[5]);
					stmt.setDouble(6, en.getValue());
					stmt.addBatch();
					count++;
					if (count % 100 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
			}
			if (hm.entrySet() != null && hm.entrySet().size() > 0 && hm.entrySet().size() % 100 != 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
	}

	public static void toMysqlOnImageIntex(Map<String, Double> hm, String time) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder insertSql = new StringBuilder();
			insertSql.append(
					"INSERT INTO T_IMAGE_INDEX(id,provinceid,platform,fastrate,indextime)" + " VALUES(null,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, Double> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String[] sArr = en.getKey().split("\\#", -1);
					stmt.setString(1, sArr[0]);
					stmt.setString(2, sArr[1]);
					stmt.setDouble(3, en.getValue());
					stmt.setString(4, time);
					stmt.addBatch();
					count++;
					if (count % 100 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
			}
			if (hm.entrySet() != null && hm.entrySet().size() > 0 && count != 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
	}

	public static void toMysqlOnEPGIndex(Map<String, Long> hm, String time) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder insertSql = new StringBuilder();
			insertSql.append("INSERT INTO T_EPG_INDEX(id,provinceid,platform,pagetype,avgdur,indextime)"
					+ " VALUES(null,?,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, Long> en : hm.entrySet()) {
				if (en != null && en.getValue() != null) {
					String[] sArr = en.getKey().split("\\#", -1);
					stmt.setString(1, sArr[0]);
					stmt.setString(2, sArr[1]);
					stmt.setString(3, sArr[2]);
					stmt.setLong(4, en.getValue());
					stmt.setString(5, time);
					stmt.addBatch();
					count++;
					if (count % 100 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
			}
			if (hm.entrySet() != null && hm.entrySet().size() > 0 && count != 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
	}
	
	public static void playSuccRateToMysql(HashMap<String, Double> hm) {
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = DBConnection.getConnection();
			StringBuilder insertSql = new StringBuilder();
			insertSql
					.append("INSERT INTO T_PLAY_SUCC_RATE(provinceID,platform,deviceProvider,fwVersion,parseTime,playSuccRate)"
							+ " VALUES(?,?,?,?,?,?)");
			conn.setAutoCommit(false);
			stmt = conn.prepareStatement(insertSql.toString());
			int count = 0;
			for (Entry<String, Double> en : hm.entrySet()) {
				if (null != en && null != en.getKey()) {
					String[] sArr = en.getKey().split("\\#", -1);
					stmt.setString(1, sArr[1]);
					stmt.setString(2, sArr[2]);
					stmt.setString(3, sArr[3]);
					stmt.setString(4, sArr[4]);
					stmt.setString(5, sArr[5]);
					stmt.setDouble(6, en.getValue());
					stmt.addBatch();
					count++;
					if (count % 100 == 0) {
						stmt.executeBatch();
						conn.commit();
						count = 0;
					}
				}
			}
			if (hm.entrySet() != null && hm.entrySet().size() > 0 && hm.entrySet().size() % 100 != 0) {
				stmt.executeBatch();
				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBConnection.closeDB(conn, stmt, null);
		}
	}

	public static void main(String[] args) {
		/*
		 * hmm.put("FIRST_FRAME#ALL#ALL#ALL#8#201606211417", 6.5);
		 * IndexToMysql.toMysql(hmm); double a=11; long b =3; DecimalFormat df =
		 * new DecimalFormat("0.0000"); System.out.println(df.format(a/b));
		 */
	}
}
