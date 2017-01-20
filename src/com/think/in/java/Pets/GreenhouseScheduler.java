package com.think.in.java.Pets;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class GreenhouseScheduler {
	// 在两个或者更多的线程访问的成员变量上使用volatile。
	// 当要访问的变量已在synchronized代码块中，或者为常量时，不必使用
	//Volatile修饰的成员变量在每次被线程访问时，都强迫从共享内存中重读该成员变量的值。
	//而且，当成员变量发生变化时，强迫线程将变化值回写到共享内存。
	//这样在任何时刻，两个不同的线程总是看到某个成员变量的同一个值
	private volatile boolean light = false;
	private volatile boolean water = false;
	private String thermostat = "DAY";

	//构造一个ScheduledThreadPoolExecutor对象，并且设置它的容量为10个
	ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(10);
	public void schedule(Runnable event,long period){
		//隔period秒后执行一次，但只会执行一次
		scheduler.schedule(event, period, TimeUnit.MILLISECONDS);
	}
	
	public void repeatAt(Runnable event,long initDelay,long period){
		//首次执行延迟2秒，之后的执行周期是1秒 scheduleAtFixedRate(mytask,2, 1,TimeUnit.SECONDS)
		scheduler.scheduleAtFixedRate(event, initDelay, period, TimeUnit.MILLISECONDS);
	}
	
	public void repeatWith(Runnable event,long initDelay,long period){
		//首次执行延迟2秒，之后从上一次任务结束到下一次任务开始时1秒  scheduleAtFixedRate(mytask,2, 1,TimeUnit.SECONDS)
		scheduler.scheduleWithFixedDelay(event, initDelay, period, TimeUnit.SECONDS);
	}
	
	public synchronized String getThermostat(){
		return thermostat;
	}
	public synchronized void setThermostat(String thermostat) {
		this.thermostat = thermostat;
	}
	
	
	class LightOn implements Runnable{
		@Override
		public void run() {
			System.out.println("turn on lights");
			light = true;
		}
	}
	
	class LightOff implements Runnable{
		@Override
		public void run() {
			System.out.println(" turn off lights");
			light = false;
		}
	}
	
	class WaterOn implements Runnable{
		@Override
		public void run() {
			System.out.println(" turn greenhouse water on");
			water = true;
		}
	}
	
	class WaterOff implements Runnable{
		@Override
		public void run() {
			System.out.println(" turn greenhouse water off");
			water = false;
		}
	}
	
	class ThermostatNight implements Runnable{
		@Override
		public void run() {
			System.out.println(" Thermostat to night setting");
			setThermostat("NIGHT");;
		}
	}
	class ThermostatDay implements Runnable{
		@Override
		public void run() {
			System.out.println(" Thermostat to day setting");
			setThermostat("DAY");
		}
	}
	
	class Bell implements Runnable{
		@Override
		public void run() {
			System.out.println(" Bing");
		}
	}
	
	class Terminal implements Runnable{
		@Override
		public void run() {
			System.out.println(" Terminaling ");
			scheduler.shutdownNow();
			
			new Thread(new Runnable(){

				@Override
				public void run() {
					for(DataPoit d : list){
						System.out.println(d);
					}
				}
				
			}).start();
		}
	}
	
	static class DataPoit{
		final Calendar time;
		final float tempreture;
		final float humidity;
		public DataPoit(Calendar time, float tempreture, float humidity) {
			this.time = time;
			this.tempreture = tempreture;
			this.humidity = humidity;
		}
		
		public String toString(){
			return time.getTime().toLocaleString() + String.format("    tempreture:%1$.1f humidity:%2$.2f",tempreture,humidity);
		}
	}
	
	private Calendar lastTime = Calendar.getInstance();
	{
		lastTime.set(Calendar.MINUTE, 30);
		lastTime.set(Calendar.SECOND, 00);
	}
	
	private float lastTemp = 65.0f;
	private int tempDirection = +1;
	private float lastHumidity = 50.0f;
	private int humidityDirection = +1;
	private Random rand = new Random(4);
	
	List<DataPoit> list = Collections.synchronizedList(new ArrayList<DataPoit>());
	
	class CollectionData implements Runnable{
		@Override
		public void run() {
			System.out.println("Collection data!");
			synchronized(GreenhouseScheduler.this){
				lastTime.set(Calendar.MINUTE,lastTime.get(Calendar.MINUTE)+30);
			}
			
			if(rand.nextInt(5) == 4){
				tempDirection = -tempDirection;
			}
			lastTemp = lastTemp + tempDirection*(1.0f + rand.nextFloat());
			
			if(rand.nextInt(5) == 4){
				humidityDirection = -humidityDirection;
			}
			lastHumidity = lastHumidity + humidityDirection*rand.nextFloat();
			
			list.add(new DataPoit((Calendar)lastTime.clone(),lastTemp,lastHumidity));
		}
		
	}
	
	public static void main(String[] args){
		GreenhouseScheduler gh = new GreenhouseScheduler();
		gh.schedule(gh.new Terminal(),5000);
		gh.repeatAt(gh.new Bell(), 0, 1000);
		gh.repeatAt(gh.new ThermostatNight(), 0, 2000);
		gh.repeatAt(gh.new LightOn(), 0, 200);
		gh.repeatAt(gh.new LightOff(), 0, 400);
		gh.repeatAt(gh.new WaterOn(), 0, 600);
		gh.repeatAt(gh.new WaterOff(), 0, 800);
		gh.repeatAt(gh.new ThermostatDay(), 0, 1400);
		System.out.println("------------");
		gh.repeatAt(gh.new CollectionData(),500,500);
		
	}
}


