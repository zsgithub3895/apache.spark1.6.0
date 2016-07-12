package com.sihuatech.sqm.spark.bean;

public class EPGResponseBean {
	/** 省份 */
	private String provinceID;

	/** 牌照方 */
	private String platform;
    
	/** 页面类型  1:首页,2:栏目页,3:详情页  */
	private String pageType;

	/** EPG响应耗时，单位微秒 */
	private long epgDur;

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

	public String getPageType() {
		return pageType;
	}

	public void setPageType(String pageType) {
		this.pageType = pageType;
	}

	public long getEpgDur() {
		return epgDur;
	}

	public void setEpgDur(long epgDur) {
		this.epgDur = epgDur;
	}

}
