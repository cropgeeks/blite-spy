package jhi.blitespy;

import java.util.*;
import java.sql.*;

import com.mysql.cj.jdbc.MysqlDataSource;
import java.io.*;

public class PullObserved
{
	private Properties props = new Properties();

	private Connection c, cLora;
	private PreparedStatement ips;

	public static void main(String[] args)
		throws Exception
	{
		PullObserved pullObserved = new PullObserved();

		pullObserved.loadProperties(args[0]);
		pullObserved.initDatabase();
		pullObserved.update();
	}

	private void loadProperties(String path)
		throws Exception
	{
		props.load(new FileReader(new File(path)));
	}

	private void initDatabase()
		throws Exception
	{
		// We need a connection to the lora database
		MysqlDataSource ds = new MysqlDataSource();
		ds.setURL(props.getProperty("loraURL"));
		ds.setUser(props.getProperty("loraUsername"));
		ds.setPassword(props.getProperty("loraPassword"));

		cLora = ds.getConnection();


		// And to the database holding the cloned observed data
		ds = new MysqlDataSource();
		ds.setURL(props.getProperty("url"));
		ds.setUser(props.getProperty("username"));
		ds.setPassword(props.getProperty("password"));

		c = ds.getConnection();

		// Create the table (if it doesn't exist yet)
		DatabaseMetaData dbm = c.getMetaData();
		ResultSet tables = dbm.getTables(null, null, "observed", null);
		if (tables.next() == false)
		{
			Statement s = c.createStatement();
			s.executeUpdate("CREATE TABLE observed ("
				+ "id INTEGER AUTO_INCREMENT PRIMARY KEY, "
				+ "location_id INTEGER, "
				+ "time TIMESTAMP, "
				+ "airTemperature REAL, "
				+ "relativeHumidity REAL, "
				+ "dailyMinTemperature REAL, "
				+ "dailyHumidityHours REAL, "
				+ "dailyMinTemperatureFlag REAL, "
				+ "dailyHumidityHoursFlag REAL, "
				+ "dailyMinTemperatureNearMissFlag REAL, "
				+ "dailyHumidityHoursNearMissFlag REAL, "
				+ "INDEX idx_time (time), "
				+ "CONSTRAINT observed_ibfk_locations FOREIGN KEY (location_id) REFERENCES locations (id) ON DELETE CASCADE);");
			s.close();
		}
	}

	private void update()
		throws Exception
	{
		// Loop over the locations
		PreparedStatement s = c.prepareStatement("SELECT * FROM locations WHERE observed=TRUE;");
		ResultSet rs = s.executeQuery();

		while (rs.next())
		{
			long id = rs.getLong("id");
			String name = rs.getString("name");
			float lat = rs.getFloat("latitude");
			float lon = rs.getFloat("longitude");
			float alt = rs.getFloat("altitude");

			System.out.println("Updating data for " + name);
			pullFromLoraDB(id);

		}

		rs.close();
		s.close();
	}

	private void pullFromLoraDB(long locationId)
		throws Exception
	{
		// Map location_id to lora device_id
		long loraDeviceId = Integer.parseInt(props.getProperty("" + locationId));
		long lastTime = 0;

		System.out.println("locationID="+locationId+" maps to " + loraDeviceId);

		// What was the last time we had an entry for?
		PreparedStatement s = c.prepareStatement("SELECT * FROM observed WHERE location_id=? ORDER BY id DESC LIMIT 1;");
		s.setLong(1, locationId);
		ResultSet rs = s.executeQuery();

		while (rs.next())
		{
			lastTime = rs.getTimestamp("time").getTime();
			break;
		}

		rs.close();
		s.close();

		System.out.println("last time is " + lastTime);


		// Prepare the statements that will be used elsewhere
		ips = c.prepareStatement("INSERT INTO observed ("
			+ "location_id, "
			+ "time, "
			+ "relativeHumidity, "
			+ "airTemperature) "
			+ "VALUES (?, ?, ?, ?);");

		s = cLora.prepareStatement("SELECT * FROM dlatm41 WHERE device_id=? AND publishedAt > FROM_UNIXTIME(?);");
		s.setLong(1, loraDeviceId);
		s.setLong(2, lastTime/1000); // (divide by 1000 because Java using ms and MYSQL using seconds)
		rs = s.executeQuery();

		System.out.println("loraDeviceId=" + loraDeviceId);

		// Pull each row from the LORA database table and use it to populate
		while (rs.next())
		{
			Timestamp timestamp = rs.getTimestamp("publishedAt");
			float airTemperature = rs.getFloat("airTemperature");
			float humidity = rs.getFloat("relativeHumidity");

			ips.setLong(1, locationId);
			ips.setTimestamp(2, timestamp);
			ips.setFloat(3, humidity);
			ips.setFloat(4, airTemperature);
			ips.execute();

			System.out.println(timestamp);
		}

		rs.close();

		s.close();
		ips.close();
	}
}