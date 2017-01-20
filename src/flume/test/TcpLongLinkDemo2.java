package flume.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TcpLongLinkDemo2 {
	private static int count = 0;
	static long start = System.currentTimeMillis();
	public static void runCount(PrintStream out) {
		try {
			// 发送数据到服务端
			//long tcount = 0;
			for (int i = 0; i < 100000; i++) {
				char c = 0x7f;
				String s0 = "3" + c + "455886829" + c + "20161209145809" + c
						+ "/030000001000/CCTV-11/c001_1481266487_1481266497.ts" + c + "173992" + c + "18" + c + "7" + c
						+ "4" + c + "53" + c + "2.1.7.12_M3" + c + "4" + c
						+ "http://111.11.121.183:6610/030000001000/CCTV-11/c001_1481266487_1481266497.ts?ispcode=9&IASHttpSessionId=SLB24065201612090602041570686&ts_min=1&srcurl=aHR0cDovLzExMS4xMS4xMjAuMTA4L2dpdHZfbGl2ZS9DQ1RWLTExL2MwMDFfMTQ4MTI2NjQ4N18xNDgxMjY2NDk3LnRz"
						+ c + "10" + c + "0" + c + "0" + c + "" + c + "177100" + c + "2432100" + c + "0" + c
						+ "111.11.121.183" + c + "192.168.0.101" + c + "2489872" + c + "37" + c + "20161209145903" + c
						+ "1" + c + "0" + c + "0" + c + "206" + c + "11670" + c + "0" + c + "0" + c + "41040857" + c
						+ "12330" + c + "0" + c + "0" + c + "15" + c + "0" + c + "0" + c + "0" + c + "-1" + c + "0" + c
						+ "0" + c + "0" + c + "0" + c + "1" + c + "0" + c + "0" + c + "0" + c + "0" + c + "0" + c + "0"
						+ c + "0" + c + "" + c + "";
				String s1 = s0;
				out.println(s0+"\n"+s1);
				count++;
			}
			System.out.println("+++++" + count);
			System.out.println("耗时" + ((System.currentTimeMillis() - start) / 1000) + "s");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws UnknownHostException, IOException {
		ExecutorService fixedThreadPool = Executors.newFixedThreadPool(100);
		// 客户端请求与10.223.138.124在20006端口建立TCP连接
		Socket client = new Socket("127.0.0.1", 2222);
		// 获取Socket的输出流，用来发送数据到服务端
		final PrintStream out = new PrintStream(client.getOutputStream());
		for (int i=0;i<100;i++) {
			fixedThreadPool.execute(new Runnable() {
				public void run() {
					runCount(out);
				}
				});
		 }
			
			if(count == 5000000){
				out.close();
				client.close();
			}
	}
	
	public static void writeFile() throws IOException{
		FileOutputStream fis = new FileOutputStream("D:\\zs.txt");
	    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fis));
		bw.append("ss");
		bw.newLine();
		bw.flush();
	}
	
}
