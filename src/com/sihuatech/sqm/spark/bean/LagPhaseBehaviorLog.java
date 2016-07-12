package com.sihuatech.sqm.spark.bean;

import java.io.Serializable;

public class LagPhaseBehaviorLog implements Serializable{
	private static final long serialVersionUID = 1L;
	private int logType; // 日志类型 取值：4
	private String hasID;//播放标识
	private String probeID; // 设备ID
	private String deviceProvider; // 终端厂商
	private String platform; // 牌照方
	private String provinceID; // 省份
	private String cityID; // 地市
	private String fwVersion; // 框架版本
	private String startSecond;	//卡顿开始时间	时间格式：YYYYMMDDHHMISS 
	private String expertID; //	卡顿主要原因 1：服务器性能问题 2：带宽不足 3：OTT终端问题 4：HTTP响应错误等
	private String freezeTime;//	卡顿时长	单位微秒，预留，0，大于3秒，后续播放器会更准
	private String URL;//	节目的URL	
	private String hasType;//	节目类型	1:HLS直播,2:HLS点播,3:MP4点播,4:TS点播等
	private String reserve1;//预留字段1
	private String reserve2;//预留字段2	
	
	public LagPhaseBehaviorLog(){}
	public LagPhaseBehaviorLog(String hasID, String deviceProvider, String platform, String provinceID,
			String fwVersion) {
		super();
		this.hasID = hasID;
		this.deviceProvider = deviceProvider;
		this.platform = platform;
		this.provinceID = provinceID;
		this.fwVersion = fwVersion;
	}
	
	public LagPhaseBehaviorLog(int logType,String probeID, String deviceProvider, String platform, String provinceID,String fwVersion) {
		super();
		this.logType=logType;
		this.probeID = probeID;
		this.deviceProvider = deviceProvider;
		this.platform = platform;
		this.provinceID = provinceID;
		this.fwVersion = fwVersion;
	}
	
	
	
	public int getLogType() {
		return logType;
	}
	public void setLogType(int logType) {
		this.logType = logType;
	}
	public String getHasID() {
		return hasID;
	}
	public void setHasID(String hasID) {
		this.hasID = hasID;
	}
	public String getProbeID() {
		return probeID;
	}
	public void setProbeID(String probeID) {
		this.probeID = probeID;
	}
	public String getDeviceProvider() {
		return deviceProvider;
	}
	public void setDeviceProvider(String deviceProvider) {
		this.deviceProvider = deviceProvider;
	}
	public String getPlatform() {
		return platform;
	}
	public void setPlatform(String platform) {
		this.platform = platform;
	}
	public String getProvinceID() {
		return provinceID;
	}
	public void setProvinceID(String provinceID) {
		this.provinceID = provinceID;
	}
	public String getCityID() {
		return cityID;
	}
	public void setCityID(String cityID) {
		this.cityID = cityID;
	}
	public String getFwVersion() {
		return fwVersion;
	}
	public void setFwVersion(String fwVersion) {
		this.fwVersion = fwVersion;
	}
	public String getStartSecond() {
		return startSecond;
	}
	public void setStartSecond(String startSecond) {
		this.startSecond = startSecond;
	}
	public String getExpertID() {
		return expertID;
	}
	public void setExpertID(String expertID) {
		this.expertID = expertID;
	}
	public String getFreezeTime() {
		return freezeTime;
	}
	public void setFreezeTime(String freezeTime) {
		this.freezeTime = freezeTime;
	}
	public String getURL() {
		return URL;
	}
	public void setURL(String uRL) {
		URL = uRL;
	}
	public String getHasType() {
		return hasType;
	}
	public void setHasType(String hasType) {
		this.hasType = hasType;
	}
	public String getReserve1() {
		return reserve1;
	}
	public void setReserve1(String reserve1) {
		this.reserve1 = reserve1;
	}
	public String getReserve2() {
		return reserve2;
	}
	public void setReserve2(String reserve2) {
		this.reserve2 = reserve2;
	}

}
