package jhi.blitespy;

import java.io.*;
import java.net.*;
import java.nio.charset.*;
import java.sql.*;
import java.time.*;

import com.google.gson.*;

import com.mysql.cj.jdbc.MysqlDataSource;

import jhi.blitespy.pojo.*;

public class GetForecasts
{
	private Connection c;
	private PreparedStatement ips;

	public static void main(String[] args)
		throws Exception
	{
		String url = args[0];
		String username = args[1];
		String password = args[2];
		String useragent = args[3];

		GetForecasts forecasts = new GetForecasts();

		forecasts.initDatabase(url, username, password);
		forecasts.update(useragent);
	}

	private void initDatabase(String url, String username, String password)
		throws Exception
	{
		MysqlDataSource ds = new MysqlDataSource();
		ds.setURL(url);
		ds.setUser(username);
		ds.setPassword(password);

		c = ds.getConnection();

		// Create the table (if it doesn't exist yet)
		DatabaseMetaData dbm = c.getMetaData();
		ResultSet tables = dbm.getTables(null, null, "forecasts", null);
		if (tables.next() == false)
		{
			Statement s = c.createStatement();
			s.executeUpdate("CREATE TABLE forecasts ("
				+ "id INTEGER AUTO_INCREMENT PRIMARY KEY, "
				+ "location_id INTEGER, "
				+ "time TIMESTAMP, "
				+ "airPressure REAL, "
				+ "airTemperature REAL, "
				+ "cloudArea REAL, "
				+ "relativeHumidity REAL, "
				+ "windDirection REAL, "
				+ "windSpeed REAL, "
				+ "precipitation REAL, "
				+ "dailyMinTemperature REAL, "
				+ "dailyHumidityHours REAL, "
				+ "dailyMinTemperatureFlag REAL, "
				+ "dailyHumidityHoursFlag REAL, "
				+ "dailyMinTemperatureNearMissFlag REAL, "
				+ "dailyHumidityHoursNearMissFlag REAL, "
				+ "INDEX idx_time (time), "
				+ "CONSTRAINT forecasts_ibfk_locations FOREIGN KEY (location_id) REFERENCES locations (id) ON DELETE CASCADE);");
			s.close();
		}

		// Prepare the statements that will be used elsewhere
		ips = c.prepareStatement("INSERT INTO forecasts ("
			+ "location_id, "
			+ "time, "
			+ "airPressure, "
			+ "airTemperature, "
			+ "cloudArea, "
			+ "relativeHumidity, "
			+ "windDirection, "
			+ "windSpeed, "
			+ "precipitation) "
			+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);");
	}

	private void update(String useragent)
		throws Exception
	{
		long now = System.currentTimeMillis();

		// Remove existing future data
		// (divide by 1000 because Java using ms and MYSQL using seconds)
		PreparedStatement delStatement = c.prepareStatement("DELETE from forecasts WHERE time > FROM_UNIXTIME("+(now/1000)+");");
		delStatement.execute();


		// Loop over the locations
		PreparedStatement s = c.prepareStatement("SELECT * FROM locations WHERE forecast = TRUE;");
		ResultSet rs = s.executeQuery();

		while (rs.next())
		{
			long id = rs.getLong(1);
			String name = rs.getString(2);
			float lat = rs.getFloat(3);
			float lon = rs.getFloat(4);
			float alt = rs.getFloat(5);

			System.out.println("Getting forecast for " + name);
			getForecast(id, lat, lon, now, useragent);

			// Wait 2s between requests
			try { Thread.sleep(2000); }
			catch (InterruptedException e) {}
		}

		rs.close();
		s.close();
	}

	private void getForecast(long id, float lat, float lon, long now, String useragent)
		throws Exception
	{
		String webPage = "https://api.met.no/weatherapi/locationforecast/2.0/compact?lat="+lat+"&lon="+lon;


		URL url = new URL(webPage);
		HttpURLConnection c = (HttpURLConnection)url.openConnection();

		c.setRequestProperty("User-Agent", useragent);
		c.setRequestProperty("Accept", "application/json");

        try (InputStream is = c.getInputStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8)))
		{
			Gson gson = new Gson();
			MetData data = gson.fromJson(reader, MetData.class);

			// Hold the values from any previous iterations of the looped data
			long prevTime = 0;
			float prevTemp = 0;
			float prevHumd = 0;

			for (TimeSeries t: data.getProperties().getTimeseries())
			{
				Details details = t.getData().getInstant().getDetails();
				Instant instant = Instant.parse(t.getTime());
				java.util.Date date = java.util.Date.from(instant);

				// Don't add entries that are before 'now', otherwise we'll end
				// up with duplicates because it's not 'forecast' info if its
				// time < current
				if (date.getTime() <= now)
					continue;

				// Check and fill in any missing hours (we always want hourly data)
				// which requires interplating the data between two timepoints (prev and curr)
				// but only for temp/humidity because that's all BlightSpy uses
				if (prevTime != 0)
				{
					long hoursMissing = (date.getTime() - prevTime)/1000/60/60;

					// Current values
					float currTemp = details.airTemperature;
					float currHumd = details.relativeHumidity;

					// And the difference from the previous values (per hour)
					float deltaTemp = (currTemp - prevTemp) / hoursMissing;
					float deltaHumd = (currHumd - prevHumd) / hoursMissing;

					// For each missing hour, add a new entry...
					for (int i = 1; i < hoursMissing; i++)
					{
						long missingTime = prevTime + (i * 1000*60*60);
						java.util.Date missingDate = new java.util.Date(missingTime);
						System.out.println("  " + missingDate);

						ips.setLong(1, id);
						ips.setTimestamp(2, new Timestamp(missingTime));
						ips.setNull(3, Types.FLOAT);
						ips.setFloat(4, prevTemp + deltaTemp*i);
						ips.setNull(5, Types.FLOAT);
						ips.setFloat(6, prevHumd + deltaHumd*i);
						ips.setNull(7, Types.FLOAT);
						ips.setNull(8, Types.FLOAT);
						ips.setNull(9, Types.FLOAT);

						ips.executeUpdate();
					}
				}

				// Now fill in the value received from MET Norway
				try
				{
					ips.setLong(1, id);
					ips.setTimestamp(2, new Timestamp(date.getTime()));
					ips.setFloat(3, details.airPressureAtSeaLevel);
					ips.setFloat(4, details.airTemperature);
					ips.setFloat(5, details.cloudAreaFraction);
					ips.setFloat(6, details.relativeHumidity);
					ips.setFloat(7, details.windFromDirection);
					ips.setFloat(8, details.windSpeed);

					if (t.getData().getNext_1_hours() != null)
						ips.setFloat(9, t.getData().getNext_1_hours().getDetails().getPrecipitationAmount());
					else
						ips.setNull(9, Types.FLOAT);

					ips.executeUpdate();
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}


				System.out.println(t.getTime());
				System.out.println("  " + details.airPressureAtSeaLevel);
				System.out.println("  " + details.airTemperature);
				System.out.println("  " + details.cloudAreaFraction);
				System.out.println("  " + details.relativeHumidity);
				System.out.println("  " + details.windFromDirection);
				System.out.println("  " + details.windSpeed);

				if (t.getData().getNext_1_hours() != null)
					System.out.println("  rain: " + t.getData().getNext_1_hours().getDetails().getPrecipitationAmount());

				prevTime = date.getTime();
				prevTemp = details.airTemperature;
				prevHumd = details.relativeHumidity;
			}
        }
	}
}