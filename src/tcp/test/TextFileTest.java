package tcp.test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;

public class TextFileTest {

	public static void main(String[] args) throws IOException {
               Employee[] em = new Employee[3];
               em[0] = new Employee("zhangsai",8000,1988,11,05);
               em[1] = new Employee("lisi",7000,1985,12,06);
               em[2] = new Employee("wangwu",6000,1980,10,15);
               try(PrintWriter out = new PrintWriter("employee.dat","UTF-8")){
            	   writeData(em,out);
               }
               try(Scanner in = new Scanner(new FileInputStream("employee.dat"),"UTF-8")){
            	   Employee[] staff = readData(in);
            	   for(Employee e:staff){
                	   System.out.println(e);
                   }
               }
	}

	private static Employee[] readData(Scanner in) {
		 int n = in.nextInt();
		 //in.nextLine();
		 Employee[] emGet = new Employee[n];
		 for(int i=0;i<n;i++){
			 String emDate = in.nextLine();
			 Employee employee = emGet[i];
			 String[] value = emDate.split("\\|");
			 employee.setName(value[0]);
			 employee.setSalary(Integer.valueOf(value[1]));
			 employee.setYear(Integer.valueOf(value[2]));
			 employee.setMonth(Integer.valueOf(value[3]));
			 employee.setDay(Integer.valueOf(value[4]));
		 }
		 return emGet;
	}

	private static void writeData(Employee[] em, PrintWriter out) {
		      out.println(em.length);
		      for(Employee ee:em){
		    	  String s = ee.getName()+"|"+ee.getSalary()+"|"+ee.getYear()+"|"+ee.getMonth()+"|"+ee.getDay();
		    	  out.println(s);
		      }
	}

}
