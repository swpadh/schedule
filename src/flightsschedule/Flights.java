package flightsschedule;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.UUID;

import org.apache.spark.sql.Row;

public class Flights implements Serializable {

	private static final long serialVersionUID = -1964457508297228407L;
	private int id;
	private int year;
	private int day_of_month;
	private Date fl_date;
	private int airline_id;
	private String carrier;
	private int fl_num;
	private int origin_airport_id;
	private String origin;
	private String origin_city_name;
	private String origin_state_abr;
	private String dest;
	private String dest_city_name;
	private String dest_state_abr;
	private Date dep_time;
	private Date arr_time;
	private Date actual_elapsed_time;
	private Date air_time;
	private int distance;

	public Flights() {
	}

	public static Flights changeOrigin(Row aRow) {
		Flights aFlights = new Flights();
		aFlights.id= (Integer)aRow.getAs("id");
		aFlights.year =  (Integer)aRow.getAs("year");
		aFlights.day_of_month =  (Integer)aRow.getAs("day_of_month");
		aFlights.fl_date =  (Date)aRow.getAs("fl_date");
		aFlights.airline_id =  (Integer)aRow.getAs("airline_id");
		aFlights.carrier = (String) aRow.getAs("carrier");
		aFlights.fl_num =  (Integer)aRow.getAs("fl_num");
		aFlights.origin_airport_id =  (Integer)aRow.getAs("origin_airport_id");
		aFlights.origin =  ((String) aRow.getAs("origin")).replace("BOS", "TST");
		aFlights.origin_city_name = (String) aRow.getAs("origin_city_name");
		aFlights.origin_state_abr =  (String)aRow.getAs("origin_state_abr");
		aFlights.dest =  (String)aRow.getAs("dest");
		aFlights.dest_city_name =  (String)aRow.getAs("dest_city_name");
		aFlights.dest_state_abr =  (String)aRow.getAs("dest_state_abr");
		aFlights.dep_time = (Date) aRow.getAs("dep_time");
		aFlights.arr_time =  (Date)aRow.getAs("arr_time");
		aFlights.actual_elapsed_time =  (Date)aRow.getAs("actual_elapsed_time");
		aFlights.air_time = (Date) aRow.getAs("air_time");
		aFlights.distance = (Integer) aRow.getAs("distance");
		return aFlights;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getDay_of_month() {
		return day_of_month;
	}

	public void setDay_of_month(int day_of_month) {
		this.day_of_month = day_of_month;
	}

	public Date getFl_date() {
		return fl_date;
	}

	public void setFl_date(Date fl_date) {
		this.fl_date = fl_date;
	}

	public int getAirline_id() {
		return airline_id;
	}

	public void setAirline_id(int airline_id) {
		this.airline_id = airline_id;
	}

	public String getCarrier() {
		return carrier;
	}

	public void setCarrier(String carrier) {
		this.carrier = carrier;
	}

	public int getFl_num() {
		return fl_num;
	}

	public void setFl_num(int fl_num) {
		this.fl_num = fl_num;
	}

	public int getOrigin_airport_id() {
		return origin_airport_id;
	}

	public void setOrigin_airport_id(int origin_airport_id) {
		this.origin_airport_id = origin_airport_id;
	}

	public String getOrigin() {
		return origin;
	}

	public void setOrigin(String origin) {
		this.origin = origin;
	}

	public String getOrigin_city_name() {
		return origin_city_name;
	}

	public void setOrigin_city_name(String origin_city_name) {
		this.origin_city_name = origin_city_name;
	}

	public String getOrigin_state_abr() {
		return origin_state_abr;
	}

	public void setOrigin_state_abr(String origin_state_abr) {
		this.origin_state_abr = origin_state_abr;
	}

	public String getDest() {
		return dest;
	}

	public void setDest(String dest) {
		this.dest = dest;
	}

	public String getDest_city_name() {
		return dest_city_name;
	}

	public void setDest_city_name(String dest_city_name) {
		this.dest_city_name = dest_city_name;
	}

	public String getDest_state_abr() {
		return dest_state_abr;
	}

	public void setDest_state_abr(String dest_state_abr) {
		this.dest_state_abr = dest_state_abr;
	}

	public Date getDep_time() {
		return dep_time;
	}

	public void setDep_time(Date dep_time) {
		this.dep_time = dep_time;
	}

	public Date getArr_time() {
		return arr_time;
	}

	public void setArr_time(Date arr_time) {
		this.arr_time = arr_time;
	}

	public Date getActual_elapsed_time() {
		return actual_elapsed_time;
	}

	public void setActual_elapsed_time(Date actual_elapsed_time) {
		this.actual_elapsed_time = actual_elapsed_time;
	}

	public Date getAir_time() {
		return air_time;
	}

	public void setAir_time(Date air_time) {
		this.air_time = air_time;
	}

	public int getDistance() {
		return distance;
	}

	public void setDistance(int distance) {
		this.distance = distance;
	}

	@Override
	public String toString() {
		return "Flights [id=" + id + ", year=" + year + ", day_of_month="
				+ day_of_month + ", fl_date=" + fl_date + ", airline_id="
				+ airline_id + ", carrier=" + carrier + ", fl_num=" + fl_num
				+ ", origin_airport_id=" + origin_airport_id + ", origin="
				+ origin + ", origin_city_name=" + origin_city_name
				+ ", origin_state_abr=" + origin_state_abr + ", dest=" + dest
				+ ", dest_city_name=" + dest_city_name + ", dest_state_abr="
				+ dest_state_abr + ", dep_time=" + dep_time + ", arr_time="
				+ arr_time + ", actual_elapsed_time=" + actual_elapsed_time
				+ ", air_time=" + air_time + ", distance=" + distance + "]";
	}

}
