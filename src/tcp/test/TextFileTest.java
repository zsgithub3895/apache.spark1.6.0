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
		 int n = in.nextInt();//在调用了nextInt()后，我们可以先调用一次nextLine(),将该行剩下的内容抛弃；
		 in.nextLine();
		 Employee[] employees = new Employee[n];
		 for(int i=0;i<n;i++){
			 String emDate = in.nextLine();
			 employees[i] = new Employee();
			 String[] value = emDate.split("\\|");
			 employees[i].setName(value[0]);
			 employees[i].setSalary(Integer.valueOf(value[1]));
			 employees[i].setYear(Integer.valueOf(value[2]));
			 employees[i].setMonth(Integer.valueOf(value[3]));
			 employees[i].setDay(Integer.valueOf(value[4]));
		 }
		 return employees;
	}

	private static void writeData(Employee[] em, PrintWriter out) {
		      out.println(em.length);
		      for(Employee ee:em){
		    	  String s = ee.getName()+"|"+ee.getSalary()+"|"+ee.getYear()+"|"+ee.getMonth()+"|"+ee.getDay();
		    	  out.println(s);
		      }
	}

}
