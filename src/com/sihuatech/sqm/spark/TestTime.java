package com.sihuatech.sqm.spark;

import java.util.Calendar;

public class TestTime {

	public static void main(String[] args) {
		String time = "2016122710";
	    	Calendar c = Calendar.getInstance();
	    	int year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
	    	int count =0;
	    	System.out.println(time.length());
	    	switch (time.length()/2) {
	    		case(7):second = Integer.valueOf(time.substring(12, 14));count++;
	    		case(6):minute = Integer.valueOf(time.substring(10, 12));count++;
	    		case(5):hour = Integer.valueOf(time.substring(8, 10));break;
	    		case(4):day = Integer.valueOf(time.substring(6, 8));count++;
	    		case(3):month = Integer.valueOf(time.substring(4, 6));count++;
	    		case(2):year = Integer.valueOf(time.substring(0, 4));count++;
	    		default:;
	    	}
	    	System.out.print(count);
	    	//c.set(year, month-1,day,hour,minute,second);

	}

}
