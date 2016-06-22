package flightsschedule;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class InsertData {
	private Cluster cluster;
	private Session session;

	private enum KIND {
		FLIGHTS, FLIGHTS_BY_ORIGIN, FLIGHTS_BY_AIRTIME
	};

	private ConcurrentHashMap<KIND, PreparedStatement> preparedMap = new ConcurrentHashMap<KIND, PreparedStatement>();
	private final SimpleDateFormat formatter = new SimpleDateFormat(
			"yyyy/MM/dd");

	public void close() {
		cluster.close();
	}

	public void connect(String node) {
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n",
				metadata.getClusterName());
		session = cluster.connect();
		metadata.getAllHosts().forEach(
				(host) -> {
					System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
							host.getDatacenter(), host.getAddress(),
							host.getRack());
				});

	}

	public Date getDate(Date flightDate, String interval) {
		Instant instant = Instant.ofEpochMilli(flightDate.getTime());
		String strTime = interval;
		String strTimeMin = "0";
		String strTimeHr = "0";
		int len = strTime.length();
		if (len == 1 || len == 2) {
			strTimeMin = strTime.substring(0, len);
		} else if (len > 2) {
			strTimeMin = strTime.substring(len - 2, len);
			strTimeHr = strTime.substring(0, len - 2);
		}
		return Date.from(instant.plus(Long.parseLong(strTimeHr),
				ChronoUnit.HOURS).plus(Long.parseLong(strTimeMin),
				ChronoUnit.MINUTES));
	}

	public BoundStatement createFlightsInitialStatement(CSVRecord csvRecord)
			throws ParseException {
		PreparedStatement statement = preparedMap
				.computeIfAbsent(
						KIND.FLIGHTS,
						(t) -> {
							return session
									.prepare("INSERT INTO flights_schedule.flights "
											+ "(ID,YEAR,DAY_OF_MONTH,FL_DATE,AIRLINE_ID,"
											+ "CARRIER,FL_NUM,ORIGIN_AIRPORT_ID,ORIGIN,ORIGIN_CITY_NAME,ORIGIN_STATE_ABR,"
											+ "DEST,DEST_CITY_NAME,DEST_STATE_ABR,DEP_TIME,ARR_TIME, ACTUAL_ELAPSED_TIME, AIR_TIME,DISTANCE) "
											+ "VALUES (?, ?, ?, ?, ?,?, ?, ?, ?, ?,?, ?, ?, ?, ?,?,?,?,?);");

						});

		Date flightDate = formatter.parse(csvRecord.get(3));
		Date depTime = getDate(flightDate, csvRecord.get(14));
		Date arrivalTime = getDate(flightDate, csvRecord.get(15));
		Date elapsedTime = getDate(flightDate, csvRecord.get(16));
		Date airTime = getDate(flightDate, csvRecord.get(17));
		BoundStatement boundStatement = new BoundStatement(statement);

		boundStatement.bind(Integer.parseInt(csvRecord.get(0)),
				Integer.parseInt(csvRecord.get(1)),
				Integer.parseInt(csvRecord.get(2)), flightDate,
				Integer.parseInt(csvRecord.get(4)), csvRecord.get(5),
				Integer.parseInt(csvRecord.get(6)),
				Integer.parseInt(csvRecord.get(7)), csvRecord.get(8),
				csvRecord.get(9), csvRecord.get(10), csvRecord.get(11),
				csvRecord.get(12), csvRecord.get(13), depTime, arrivalTime,
				elapsedTime, airTime, Integer.parseInt(csvRecord.get(18)));
		return boundStatement;
	}

	public BoundStatement createFlightsByOriginStatement(CSVRecord csvRecord)
			throws ParseException {
		PreparedStatement statement = preparedMap
				.computeIfAbsent(
						KIND.FLIGHTS_BY_ORIGIN,
						(t) -> {
							return session
									.prepare("INSERT INTO flights_schedule.flights_by_origin "
											+ "(ID,YEAR,DAY_OF_MONTH,FL_DATE,AIRLINE_ID,"
											+ "CARRIER,FL_NUM,ORIGIN_AIRPORT_ID,ORIGIN,ORIGIN_CITY_NAME,ORIGIN_STATE_ABR,"
											+ "DEST,DEST_CITY_NAME,DEST_STATE_ABR,DEP_TIME,ARR_TIME, ACTUAL_ELAPSED_TIME, AIR_TIME,DISTANCE) "
											+ "VALUES (?, ?, ?, ?, ?,?, ?, ?, ?, ?,?, ?, ?, ?, ?,?,?,?,?);");
						});

		Date flightDate = formatter.parse(csvRecord.get(3));
		Date depTime = getDate(flightDate, csvRecord.get(14));
		Date arrivalTime = getDate(flightDate, csvRecord.get(15));
		Date elapsedTime = getDate(flightDate, csvRecord.get(16));
		Date airTime = getDate(flightDate, csvRecord.get(17));
		BoundStatement boundStatement = new BoundStatement(statement);

		boundStatement.bind(Integer.parseInt(csvRecord.get(0)),
				Integer.parseInt(csvRecord.get(1)),
				Integer.parseInt(csvRecord.get(2)), flightDate,
				Integer.parseInt(csvRecord.get(4)), csvRecord.get(5),
				Integer.parseInt(csvRecord.get(6)),
				Integer.parseInt(csvRecord.get(7)), csvRecord.get(8),
				csvRecord.get(9), csvRecord.get(10), csvRecord.get(11),
				csvRecord.get(12), csvRecord.get(13), depTime, arrivalTime,
				elapsedTime, airTime, Integer.parseInt(csvRecord.get(18)));
		return boundStatement;
	}

	public BoundStatement createFlightsByAirtimeStatement(CSVRecord csvRecord)
			throws ParseException {
		PreparedStatement statement = preparedMap
				.computeIfAbsent(
						KIND.FLIGHTS_BY_AIRTIME,
						(t) -> {
							return session
									.prepare("INSERT INTO flights_schedule.flights_by_airtime_bucket "
											+ "(ID,YEAR,DAY_OF_MONTH,FL_DATE,AIRLINE_ID,"
											+ "CARRIER,FL_NUM,ORIGIN_AIRPORT_ID,ORIGIN,ORIGIN_CITY_NAME,ORIGIN_STATE_ABR,"
											+ "DEST,DEST_CITY_NAME,DEST_STATE_ABR,DEP_TIME,ARR_TIME, ACTUAL_ELAPSED_TIME, AIR_TIME,AIR_TIME_BUCKET_START,AIR_TIME_BUCKET_END, DISTANCE) "
											+ "VALUES (?, ?, ?, ?, ?,?, ?, ?, ?, ?,?, ?, ?, ?, ?,?,?,?,?,?,?);");

						});
		Date flightDate = formatter.parse(csvRecord.get(3));
		Date depTime = getDate(flightDate, csvRecord.get(14));
		Date arrivalTime = getDate(flightDate, csvRecord.get(15));
		Date elapsedTime = getDate(flightDate, csvRecord.get(16));
		String strAirTime = csvRecord.get(17);
		int nAirTime = Integer.parseInt(strAirTime);
		Date airTime = getDate(flightDate, strAirTime);
		BoundStatement boundStatement = new BoundStatement(statement);

		int airTimeBucketStart = nAirTime - (nAirTime % 10);
		int airTimeBucketEnd = airTimeBucketStart + 10;

		boundStatement.bind(Integer.parseInt(csvRecord.get(0)),
				Integer.parseInt(csvRecord.get(1)),
				Integer.parseInt(csvRecord.get(2)), flightDate,
				Integer.parseInt(csvRecord.get(4)), csvRecord.get(5),
				Integer.parseInt(csvRecord.get(6)),
				Integer.parseInt(csvRecord.get(7)), csvRecord.get(8),
				csvRecord.get(9), csvRecord.get(10), csvRecord.get(11),
				csvRecord.get(12), csvRecord.get(13), depTime, arrivalTime,
				elapsedTime, airTime, airTimeBucketStart, airTimeBucketEnd,
				Integer.parseInt(csvRecord.get(18)));
		return boundStatement;
	}

	public void loadDataToQueryTables() {
		File csvData = new File(
				"resources/flights_from_pg.csv");
		try {
			CSVParser parser = CSVParser.parse(csvData,
					StandardCharsets.US_ASCII, CSVFormat.RFC4180);
			parser.forEach((csvRecord) -> {
				try {
					BatchStatement batchStatement = new BatchStatement(
							BatchStatement.Type.LOGGED);
					System.out.println(csvRecord);
					batchStatement
							.add(createFlightsByOriginStatement(csvRecord));
					batchStatement
							.add(createFlightsByAirtimeStatement(csvRecord));
					session.execute(batchStatement);
				} catch (ParseException ex) {
					ex.printStackTrace();
				}
			});
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}

	}

	public void loadFlightsData() {
		File csvData = new File(
				"resouces/flights_from_pg.csv");
		try {
			CSVParser parser = CSVParser.parse(csvData,
					StandardCharsets.US_ASCII, CSVFormat.RFC4180);
			parser.forEach((csvRecord) -> {
				try {		
					System.out.println(csvRecord);
					session.execute(createFlightsInitialStatement(csvRecord));
				} catch (ParseException ex) {
					ex.printStackTrace();
				}
			});

		} catch (IOException ioe) {
			ioe.printStackTrace();
		}

	}

	public static void main(String[] args) {
		InsertData client = new InsertData();
		client.connect("127.0.0.1");
		//client.loadFlightsData();
		client.loadDataToQueryTables();
		client.close();

	}

}
