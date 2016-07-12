package com.sihuatech.sqm.spark.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.log4j.Logger;

public class DateUtil {
    private static final Logger logger = Logger.getLogger(DateUtil.class);
    private static final String formatter = "yyyy-MM-dd HH:mm:ss";

    public static String getCurrentDateTime(String formatter) {
        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(formatter);
        return simpleDateFormat.format(date);
    }

    public static String formatDate(Date date,String formatter) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(formatter);
        return simpleDateFormat.format(date);
    }
    public static String getCurrentDateTime() {
        Date date = new Date();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(formatter);
        return simpleDateFormat.format(date);
    }

    public static Long dateTimeToLong(String date) {
        SimpleDateFormat sdf = new SimpleDateFormat(formatter);
        Date resDate = null;
        try {
            resDate = sdf.parse(date);
        } catch (ParseException e) {
            logger.error("将日期解析成Long型异常!", e);
            return (long) -1;
        }
        if (resDate != null) {
            return resDate.getTime();
        } else {
            return (long) -1;
        }
    }

    public static Long dateTimeToLong(String date, String formatter) {
        SimpleDateFormat sdf = new SimpleDateFormat(formatter);
        Date resDate = null;
        try {
            resDate = sdf.parse(date);
        } catch (ParseException e) {
            logger.error("将日期解析成Long型异常!", e);
            return (long) -1;
        }
        if (resDate != null) {
            return resDate.getTime();
        } else {
            return (long) -1;
        }
    }
    public static Date formatDateTime(String date,String formatter) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(formatter);
        try {
            return simpleDateFormat.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }
    public static Date addDay(Date date, int day) {
        return DateUtil.add(date, day, "day");
    }
    public static Date add(Date date, int num, String mode) {
        GregorianCalendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        if (mode.equalsIgnoreCase("month")) {
            calendar.add(Calendar.MONTH, num);
        } else {
            calendar.add(Calendar.DAY_OF_MONTH, num);
        }
        return calendar.getTime();
    }
    public static String timeToCron(String time){
        String result=null;
        if(time!=null){
            String str[]=time.split(":");
            if(str==null|str.length<3){
                return null;
            }
            String hh=str[0];
            String mi=str[1];
            String ss=str[2];
            result=String.format("%s %s %s * * ? *", ss,mi,hh);
        }
        return result;
    }
    public static int dateDiff(String fromDate, String toDate) throws ParseException {
        int days = 0;
        SimpleDateFormat df = new SimpleDateFormat(formatter);
        Date from = df.parse(fromDate);
        Date to = df.parse(toDate);
        days = (int) Math.abs((to.getTime() - from.getTime()) / (60 * 60 * 1000));
        return days;
    }
    public static int dayDiff(String fromDate, String toDate,String formatter) throws ParseException {
        int days = 0;
        SimpleDateFormat df = new SimpleDateFormat(formatter);
        Date from = df.parse(fromDate);
        Date to = df.parse(toDate);
        //开始时间和结束时间之间有多少天
        days = (int) Math.abs((to.getTime() - from.getTime()) / (1000*60 * 60 * 24));
        return days;
    }
    public static int getDiffSeconds(String fromDate, String toDate) {
        try {
            int seconds = 0;
            SimpleDateFormat df = new SimpleDateFormat(formatter);
            Date from = df.parse(fromDate);
            Date to = df.parse(toDate);
            seconds = (int) Math.abs((to.getTime() - from.getTime()) / 1000);
            return seconds;
        }catch (Exception ex){
            ex.printStackTrace();
            return -1;
        }
    }
    public static int getSeconds(String date, String formatter){
        try {
            int seconds = 0;
            SimpleDateFormat df = new SimpleDateFormat(formatter);
            Date to = df.parse(date);

            Calendar fromDate = Calendar.getInstance();
            fromDate.setTime(to);
            fromDate.set(Calendar.HOUR_OF_DAY, 0);
            fromDate.set(Calendar.MINUTE, 0);
            fromDate.set(Calendar.SECOND, 0);
            fromDate.set(Calendar.MILLISECOND, 0);
            seconds = (int) Math.abs((to.getTime() - fromDate.getTime().getTime()) / 1000);
            return seconds;
        }catch (Exception ex){
            ex.printStackTrace();
            return -1;
        }
    }
    public static String getDateBySeconds(String baseTime,int seconds){
        try {
            int sencond=seconds%60;
            int minute=(seconds%3600)/60;
            int hour=seconds/3600;

            Date current = DateUtil.formatDateTime(baseTime,formatter);
            Calendar fromDate = Calendar.getInstance();
            fromDate.setTime(current);
            fromDate.set(Calendar.HOUR_OF_DAY, hour);
            fromDate.set(Calendar.MINUTE, minute);
            fromDate.set(Calendar.SECOND, sencond);
            fromDate.set(Calendar.MILLISECOND, 0);
            return formatDate(fromDate.getTime(),formatter);
        }catch (Exception ex){
            ex.printStackTrace();
            return null;
        }
    }
    public static int getSeconds(String date) {
        return getSeconds(date,formatter);
    }


    public static String addDay(String date,String formatter, int day) {
        return formatDate(DateUtil.add(formatDateTime(date,formatter), day, "day"),formatter);
    }

}