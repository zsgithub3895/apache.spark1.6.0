package tcp.test;

public class Employee {
       private String name;
       private int salary;
       private int year;
       private int month;
       private int day;
	public Employee(String name, int salary, int year, int month, int day) {
		super();
		this.name = name;
		this.salary = salary;
		this.year = year;
		this.month = month;
		this.day = day;
	}
	public String getName() {
		return name;
	}
	public int getSalary() {
		return salary;
	}
	public int getYear() {
		return year;
	}
	public int getMonth() {
		return month;
	}
	public int getDay() {
		return day;
	}
	public void setName(String name) {
		this.name = name;
	}
	public void setSalary(int salary) {
		this.salary = salary;
	}
	public void setYear(int year) {
		this.year = year;
	}
	public void setMonth(int month) {
		this.month = month;
	}
	public void setDay(int day) {
		this.day = day;
	}	
	
}
