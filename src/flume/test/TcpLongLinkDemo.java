package flume.test;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import kafka.producer.KeyedMessage;

public class TcpLongLinkDemo extends Thread{
	public static void main(String[] args) {
		long start = System.currentTimeMillis();
		try {
			runCause();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*try {
			// 客户端请求与10.223.138.124在20006端口建立TCP连接
			Socket client = new Socket("127.0.0.1", 2222);
			//Socket client = new Socket("10.223.138.153", 5888);
			// 获取Socket的输出流，用来发送数据到服务端
			PrintStream out = new PrintStream(client.getOutputStream());
			// 发送数据到服务端
			//int k = 0;
			Random r = new Random();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			File inFile =new File("D:\\playrequest.csv");
			BufferedReader br = new BufferedReader(new FileReader(inFile));
			String inString="";
			int count=1;
			//Map<String,Integer> map = new HashMap<String,Integer>();
			while((inString=br.readLine()) !=  null){
				char c = 0x7F;
				String[] ex = inString.split(String.valueOf(c),-1);
				String probeID = ex[2];
				if("23".equals(ex[7])){
					//map.put(probeID,count);
					out.println(inString);
				};
			}
			System.out.println("耗时" + ((System.currentTimeMillis() - start) / 1000) + "s");
			out.close();
			client.close();
		} catch (Exception e) {
			e.printStackTrace();
		} */
	}
	
    public static void runCause() throws IOException {
		long start = System.currentTimeMillis();
			// 客户端请求与10.223.138.124在20006端口建立TCP连接
			//Socket client = new Socket("127.0.0.1", 2222);
			//Socket client = new Socket("10.223.138.153", 5888);
			// 获取Socket的输出流，用来发送数据到服务端
			//PrintStream out = new PrintStream(client.getOutputStream());
			// 发送数据到服务端
			//int k = 0;
			boolean flag = true;
			Random r = new Random();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			File inFile =new File("D:\\playrequest.csv");
			BufferedReader br = new BufferedReader(new FileReader(inFile));
			Map<String,String> hasIDMap = new HashMap<String,String>();
			Map<String,String> probeIDMap =new HashMap<String,String>();
			int count=0;
			String inString="";
			while((inString=br.readLine()) !=  null){
				char c = 0x7F;
				String[] ex = inString.split(String.valueOf(c),-1);
				String hasId = ex[1];
				String probeID = ex[4];
				String provinceID = ex[7];
				String playSecond = ex[12];
				String freezTime= ex[14];
				String kpiutcsec = ex[23];
				String value = kpiutcsec+"|"+probeID+"|"+freezTime+"|"+playSecond;
					if("23".equals(provinceID)){
						  String vv = hasIDMap.get(hasId);
				        	 if(null != vv && hasIDMap.containsKey(hasId)){
				        		 String kpiutcsecTwo=vv.split("\\|")[0];
				        		 if(kpiutcsec.compareTo(kpiutcsecTwo) > 0){
							    		hasIDMap.put(hasId, value);
						    	   }else if(kpiutcsec.compareTo(kpiutcsecTwo) == 0){
						    		  System.out.println("++++++++++++"+kpiutcsecTwo); 
						    	   }
				        	 }else{
				        		 hasIDMap.put(hasId, value);
				        	 }
				        	 count++;
						}
					
				}
			//System.out.println("+过滤前+"+count);
			System.out.println("+过滤后+"+hasIDMap.size());
			
			for(Entry<String, String> en:hasIDMap.entrySet()){
				String probeIDTwo = en.getValue().split("\\|")[1];
				long freezTimeTwo =Long.valueOf(en.getValue().split("\\|")[2]);
				long playSecondTwo =Long.valueOf(en.getValue().split("\\|")[3]);
				String v = probeIDMap.get(probeIDTwo);
				if(null != v && probeIDMap.containsKey(probeIDTwo)){
					long freezTime3 =Long.valueOf(v.split("\\|")[0]);
					long playSecond3 =Long.valueOf(v.split("\\|")[1]);
					freezTimeTwo = freezTimeTwo+freezTime3;
					playSecondTwo = playSecondTwo+playSecond3;
				}
					probeIDMap.put(probeIDTwo, freezTimeTwo+"|"+playSecondTwo);
			 }
			
			int t1=0;
			int t2=0;
			int t3=0;
			int t4=0;
			
			//System.out.println("++++"+probeIDMap.size());
			for(Entry<String, String> enen:probeIDMap.entrySet()){
				long r1= Long.valueOf(enen.getValue().split("\\|")[0]);
				long r2= Long.valueOf(enen.getValue().split("\\|")[1]);
				double result = (r1*1.0d)/r2;
				if(result > 50000){
					t1++;
				}
				if(result>10000 && result<=50000){
					t2 ++;
				}
				
				if(result > 0 && result<=10000){
					t3 ++;
				}
				
				if(result == 0){
					t4 ++;
				}
				
			}
				//out.println(inString);
			System.out.println(t1);
			System.out.println(t2);
			System.out.println(t3);
			System.out.println(t4);
    }
}