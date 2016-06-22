package flightsschedule;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

import java.time.LocalDate;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.hadoop.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.cassandra.CassandraSQLContext;

public class SparkCassandra {
	public static Row getMostFlightFromOrigin(CassandraSQLContext sqlContext,
			String flightDate) {
		sqlContext.setKeyspace("flights_schedule");
		DataFrame flightDF = sqlContext
				.sql("SELECT * from flights_by_origin WHERE fl_date= '"
						+ flightDate + "'");
		// flightDF.registerTempTable("flights");
		DataFrame flightNumDF = flightDF.rollup("origin").count();
		Row[] flightsArray = flightNumDF.collect();
		int len = flightsArray.length;
		Row maxRow = flightsArray[0];
		for (int i = 1; i < len; ++i) {
			if (flightsArray[i].get(0) != null) {
				if (((Long) flightsArray[i].get(1)).intValue() > ((Long) maxRow
						.get(1)).intValue()) {
					maxRow = flightsArray[i];
				}
			}
		}
		return maxRow;
	}

	public static long getFlightsOrginatedFromAirport(
			CassandraSQLContext sqlContext, String origin, String flightDate) {
		sqlContext.setKeyspace("flights_schedule");
		DataFrame origins = sqlContext
				.sql("SELECT * FROM flights_by_origin WHERE origin='" + origin
						+ "' and fl_date= '" + flightDate + "'");
		return origins.javaRDD().count();
	}

	public static void updateAirportCode(CassandraSQLContext sqlContext,
			String origin) {
		sqlContext.setKeyspace("flights_schedule");
		DataFrame allFlightsDF = sqlContext
				.sql("SELECT * FROM flights_by_origin where origin='BOS'");
		JavaRDD<Flights> replacedRDD = allFlightsDF.javaRDD().map(
				new Function<Row, Flights>() {

					@Override
					public Flights call(Row aRow) throws Exception {
						return Flights.changeOrigin(aRow);
					}
				});
		javaFunctions(replacedRDD).writerBuilder("flights_schedule",
				"flights_by_origin", mapToRow(Flights.class)).saveToCassandra();
	}

	public static long getFlightsOriginMatch(SparkContext sparkContext,
			String origin) {
		JavaRDD<String> rdd = javaFunctions(sparkContext)
				.cassandraTable("flights_schedule", "flights",
						mapRowTo(Flights.class)).filter((flights) -> {
					if (flights.getOrigin().startsWith(origin)) {
						return true;
					}
					return false;
				}).map(new Function<Flights, String>() {

					@Override
					public String call(Flights flights) throws Exception {
						return flights.getOrigin();
					}
				});
		if (rdd != null)
			return rdd.distinct().count();
		else
			return 0;
	}

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf(true)
				.set("spark.cassandra.connection.host", "127.0.0.1")
				.set("spark.cassandra.connection.port", "9042")
				.set("spark.cassandra.auth.username", "cassandra")
				.set("spark.cassandra.auth.password", "cassandra")
				.setAppName("schedule").setMaster("local[*]");
		SparkContext sc = new SparkContext(sparkConf);
		// cassandraSQLContext(sc);
		CassandraSQLContext cc = new CassandraSQLContext(sc);
		LocalDate localDate = LocalDate.of(2012, 1, 25);
		long number = getFlightsOrginatedFromAirport(cc, "HNL",
				localDate.toString());
		System.out
				.println("The number of flights from HNL on 2012-01-25 is  -->  "
						+ number);

		long number2 = getFlightsOriginMatch(sc, "A");
		System.out
				.println("The number of airport codes start with the letter ‘A’ is  -->  "
						+ number2);
		LocalDate localDate2 = LocalDate.of(2012, 1, 23);
		Row maxRow = getMostFlightFromOrigin(cc, localDate2.toString());
		System.out
				.println(" Originating airport had the most flights on 2012-01-23  --> "
						+ maxRow);

		updateAirportCode(cc, "BOS");
	}
}
