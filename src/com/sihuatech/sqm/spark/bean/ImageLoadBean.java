package com.sihuatech.sqm.spark.bean;

public class ImageLoadBean {
	/** 省份 */
	private String provinceID;
	
	/** 牌照方 */
	private String platform;

	/** 图片加载耗时，单位微秒 */
	private long pictureDur;

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

	public long getPictureDur() {
		return pictureDur;
	}

	public void setPictureDur(long pictureDur) {
		this.pictureDur = pictureDur;
	}

}
