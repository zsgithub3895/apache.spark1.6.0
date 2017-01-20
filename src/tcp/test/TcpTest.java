package tcp.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TcpTest {
	static long start = System.currentTimeMillis();
	static int count = 0;
	public static void runCountPlayRequest(PrintStream out) {
		try {
			// 发送数据到服务端
			for (int i = 0; i < 6000000; i++) {
				char c = 0x7f;
				String s0 = "3" + c + UUID.randomUUID() + c + "20161209145809" + c
						+ "/030000001000/CCTV-11/c001_1481266487_1481266497.ts" + c + "173992" + c + "18" + c + "7" + c
						+ "4" + c + "53" + c + "2.1.7.12_M3" + c + "4" + c
						+ "http://111.11.121.183:6610/030000001000/CCTV-11/c001_1481266487_1481266497.ts?ispcode=9&IASHttpSessionId=SLB24065201612090602041570686&ts_min=1&srcurl=aHR0cDovLzExMS4xMS4xMjAuMTA4L2dpdHZfbGl2ZS9DQ1RWLTExL2MwMDFfMTQ4MTI2NjQ4N18xNDgxMjY2NDk3LnRz"
						+ c + "10" + c + "0" + c + "0" + c + "" + c + "177100" + c + "2432100" + c + "0" + c
						+ "111.11.121.183" + c + "192.168.0.101" + c + "2489872" + c + "37" + c + Calendar.getInstance().getTimeInMillis() + c
						+ "1" + c + "0" + c + "0" + c + "206" + c + "11670" + c + "0" + c + "0" + c + "41040857" + c
						+ "12330" + c + "0" + c + "0" + c + "15" + c + "0" + c + "0" + c + "0" + c + "-1" + c + "0" + c
						+ "0" + c + "0" + c + "0" + c + "1" + c + "0" + c + "0" + c + "0" + c + "0" + c + "0" + c + "0"
						+ c + "0" + c + "" + c + "";
				out.println(s0);
			}
			System.out.println("耗时" + ((System.currentTimeMillis() - start) / 1000) + "s");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		testTerinalStateTest();
		   if(args.length <= 1){
			   System.out.println("参数不能为空！并且参数个数要大于1");
			   System.exit(0);
		   }
		   if(args.length == 2){
			   int logtype = Integer.valueOf(args[0]);
			   int port = Integer.valueOf(args[1]);
			   if(logtype == 0){
				  // testTerinalStateTest(logtype,port);
			   }if(logtype == 2){
				   testTerinalState(logtype,port);
			   }else if(logtype == 3){
				   testPlayRequest(logtype,port);
			   }else if(logtype == 4){
				   char c = 0x7f;
				   String s0 ="4"+c+"450347469"+c+"72303"+c+"47"+c+"7"+c+"4"+c+"53"+c+"2.1.7.12_M3"+c+"20161207192207"+c+"14"+c+"4932539"+c+"http://111.11.121.38:6610/000000000000/9001363343/5.m3u8?HlsSubType=1&version=1.0&IASHttpSessionId=SLB11032201612071101324689916&HlsProfileId=2&cmcc_cid=9001363343&usergroup=g02002100000&userToken=f1844fbfec6630877c2340b9fe493e05&gUserTargetIp=117.144.248.61&ispcode=1&stbId=DB3405FF00473440000074978190ABBB&gUserTargetPort=8080&srcurl=aHR0cDovL2dzbGJidG9zLml0di5jbXZpZGVvLmNuOjgwODAvd2g3ZjQ1NGM0NnR3MjUxNzI4NzQwOF8xOTk2Nzk3NDUwLzAwMDAwMDAwMDAwMC9qc21ldGEudmlkZW8uZ2l0di50di8yMDMzNjQ2MDEvNTc2MjEyMDAwLzUubTN1OD9IbHNTdWJUeXBlPTEmdXNlclRva2VuPThhMTE4NjRlNWVlMTMxMjYxNDFhYzdlYzUzODhkOGYzJmdVc2VyVGFyZ2V0SXA9MTE3LjE0NC4yNDguNjEmc3RiSWQ9M0EzNDA1RkYwMDQ3MzQ0MDAwMDA3NDk3ODE5MEFBNzkmZ1VzZXJUYXJnZXRQb3J0PTgwODAmY21jY19jaWQ9OTAwMTM2MzM0MyZIbHNQcm9maWxlSWQ9MiZobHNfYWJzX3VybD0xJmNoYW5uZWwtaWQ9eWd5aCZDb250ZW50aWQ9OTAwMTM2MzM0MyZ1c2VyZ3JvdXA9ZzA1MDMxMTAwMDAw&channel-id=ygyh&hls_abs_url=1&m3u8_level=2&Contentid=9001363343&AuthInfo=rHc1FdvCpUPqa3JpIC11Vc%2F%2F%2BEslit7d%2FlYORr50zAYw8gSOua86i%2FnUoFpWV7cQ9%2Bi6pxNIfZV9bZThhcRcez4vK"+c+"2"+c+""+c+"";
				   testAll(logtype,port,s0);
			   }else if(logtype == 5){
				   char c = 0x7f;
				   String s0 ="";
				   testAll(logtype,port,s0);
			   }else if(logtype == 6){
				   char c = 0x7f;
				   String s0 ="6"+c+"141578"+c+"nanguang"+c+"11"+c+"193"+c+"10.223.138.153"+c+"1"+c+"20161201110000"+c+"231"+c+"45"+c+""+c+"";
				   testAll(logtype,port,s0);
			   }else if(logtype == 7){
				   char c = 0x7f;
				   String s0 ="7"+c+"595049"+c+"3"+c+"24"+c+"379"+c+"http://117.144.248.72:80//box_api/SW_sns_setObject.php?type=add&userid=24457535&opertype=watched&objectid=3127720&objectname=46_%E5%92%B1%E4%BB%AC%E7%9B%B8%E7%88%B1%E5%90%A7&objectaction=GetMoiveDetail&objectactionurl=&objecttype=tv&objectext=http://images.is.ysten.com:8080/images/ysten/images/icntv2/images/2016/11/09/5af8804596c34aba9fede4cd601339b8.jpg"+c+"%E4%B8%AD%E5%9B%BD%E5%A4%A7%E9%99%86"+c+"458153744M&objectparam=&objecactor=%E5%BC%A0%E9%9D%99%E5%88%9D"+c+"%E5%BC%A0%E6%AD%86%E8%89%BA"+c+"%E7%A7%A6%E5%B2%9A"+c+"%E8%A2%81%E5%BC%98&epgid=3127720&datePoint=0&detailsId=45&titledata={type:\'HD\',picurl:\'http://images.is.ysten.com:8080/images/ysten/images/icntv2/images/2016/11/09/5af8804596c34aba9fede4cd601339b8.jpg\',title:\'46_%E5%92%B1%E4%BB%AC%E7%9B%B8%E7%88%B1%E5%90%A7\',actor:\'%E5%BC%A0%E9%9D%99%E5%88%9D"+c+"%E5%BC%A0%E6%AD%86%E8%89%BA"+c+"%E7%A7%A6%E5%B2%9A"+c+"%E8%A2%81%E5%BC%98\',url:\'\',objectid:16976517,epgid:3127720}&t=0"+c+"914119959808886"+c+"20161209023806"+c+"0"+c+""+c+"";
				   testAll(logtype,port,s0);
			   }
		   }
	}
	
	public static void testPlayRequest(int logtype,int port) throws Exception{
		final ExecutorService fixedThreadPool = Executors.newFixedThreadPool(100);
		// 客户端请求与10.223.138.124在20006端口建立TCP连接
		//Socket client = new Socket("127.0.0.1", 1111); 
		//port:5142,5143
		Socket client = new Socket("10.200.17.95", port);
		// 获取Socket的输出流，用来发送数据到服务端
		final PrintStream out = new PrintStream(client.getOutputStream());
		for (int i=0;i<100;i++) {
			fixedThreadPool.execute(new Runnable() {
				public void run() {
					runCountPlayRequest(out);
				}
				});
			Thread.sleep(1000);
		 }
			fixedThreadPool.shutdown();
	}
	public static void testAll(int logtype,int port,final String s0) throws Exception{
		final ExecutorService fixedThreadPool = Executors.newFixedThreadPool(100);
		Socket client = new Socket("10.200.17.95", port);
		// 获取Socket的输出流，用来发送数据到服务端
		final PrintStream out = new PrintStream(client.getOutputStream());
		for (int i=0;i<100;i++) {
			fixedThreadPool.execute(new Runnable() {
				public void run() {
					runCountAll(out,s0);
				}
			});
		}
		fixedThreadPool.shutdown();
	}
	
	public static void testTerinalState(int logtype,int port) throws Exception{
		final ExecutorService fixedThreadPool = Executors.newFixedThreadPool(100);
		// 客户端请求与10.223.138.124在20006端口建立TCP连接
		//Socket client = new Socket("127.0.0.1", 1111);
		Socket client = new Socket("10.200.17.95", port);
		// 获取Socket的输出流，用来发送数据到服务端
		final PrintStream out = new PrintStream(client.getOutputStream());
		for (int i=0;i<100;i++) {
			fixedThreadPool.execute(new Runnable() {
				public void run() {
					runCountTerinalState(out);
				}
			});
			Thread.sleep(1000);
		}
		fixedThreadPool.shutdown();
	}
	
	public static synchronized void runCountTerinalState(PrintStream out) {
		try {
			// 发送数据到服务端
			for (int i = 0; i < 2650872; i++) {
				char c = 0x7f;
				String s0 ="2"+c+""+i+""+c+"33"+c+"2"+c+"20161215015909"+c+"66"+c+"77";
				count ++;
				out.println(s0);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void testTerinalStateTest() throws Exception{
		final ExecutorService fixedThreadPool = Executors.newFixedThreadPool(2);
		// 客户端请求与10.223.138.124在20006端口建立TCP连接
		//Socket client = new Socket("10.200.17.95", port);
		Socket client = new Socket("127.0.0.1", 1111);
		// 获取Socket的输出流，用来发送数据到服务端
		final PrintStream out = new PrintStream(client.getOutputStream());
		for (int i=0;i<10;i++) {
			fixedThreadPool.execute(new Runnable() {
				public void run() {
					runCountTerinalStateTest(out);
				}
			});
		}
		fixedThreadPool.shutdown();
	}
	
	public static void runCountTerinalStateTest(PrintStream out) {
		try {
			// 发送数据到服务端
			for (int i = 0; i < 10; i++) {
				char c = 0x7f;
				String s0 ="2"+c+""+i+""+c+"33"+c+"2"+c+"20161215015909"+c+"66"+c+"77";
				File dir = new File("/data/zs.txt");
				FileOutputStream fis = new FileOutputStream(dir);
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fis));
				bw.append("状态日志："+s0);
				bw.newLine();
				bw.flush();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void runCountAll(PrintStream out,String s0) {
		try {
			// 发送数据到服务端
			for (int i = 0; i < 100000000; i++) {
				char c = 0x7f;
				out.println(s0);
			}
			System.out.println("耗时" + ((System.currentTimeMillis() - start) / 1000) + "s");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
