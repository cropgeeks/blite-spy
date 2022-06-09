package jhi.blitespy;

import java.sql.*;
import java.util.*;

import com.mysql.cj.jdbc.MysqlDataSource;

public class Calculate
{
	private Connection c;
	private PreparedStatement ips;

	public static void main(String[] args)
		throws Exception
	{
		String url = args[0];
		String username = args[1];
		String password = args[2];

		Calculate calculator = new Calculate();

		calculator.initDatabase(url, username, password);
		calculator.runMETCalculations();
		calculator.runObservedCalculations();
	}

	private void initDatabase(String url, String username, String password)
		throws Exception
	{
		MysqlDataSource ds = new MysqlDataSource();
		ds.setURL(url);
		ds.setUser(username);
		ds.setPassword(password);

		c = ds.getConnection();
	}

	private void runMETCalculations()
		throws Exception
	{
		// Loop over the locations
		PreparedStatement s = c.prepareStatement("SELECT * FROM locations WHERE forecast = TRUE;");
		ResultSet rs = s.executeQuery();

		while (rs.next())
		{
			long id = rs.getLong(1);
			String name = rs.getString(2);

			System.out.println("Running calculations for " + name);
			calculateMET(id);
		}

		rs.close();
		s.close();
	}

	private void calculateMET(long locationId)
		throws Exception
	{
		// Process up to 10 days' worth of forecast information
		for (int i = 0; i < 10; i++)
		{
			System.out.println("Checking day " + i);

			ips = c.prepareStatement("SELECT * FROM forecasts WHERE DATE(time) = DATE_ADD(CURDATE(), INTERVAL ? DAY) AND location_id=?;");
			ips.setInt(1, i);
			ips.setLong(2, locationId);

			int humidityHoursCount = 0;
			float minTemperature = Float.MAX_VALUE;

			ResultSet rs = ips.executeQuery();
			while (rs.next())
			{
				System.out.println("  time: " + rs.getTimestamp(3));

				float temperature = rs.getFloat(5);
				float humidity = rs.getFloat(7);

				System.out.println("    " + temperature + "c, " + humidity + "%");

				// How many hours have a humidity value higher than 90%
				if (humidity >= 90f)
					humidityHoursCount++;

				// Track the minimum temperature for the day
				minTemperature = Math.min(minTemperature, temperature);
			}
			rs.close();

			System.out.println("humidityHoursCount=" + humidityHoursCount);
			System.out.println("minTemperature=" + minTemperature);

			// Now update the rows based on our calculations
			ips = c.prepareStatement("UPDATE forecasts SET "
				+ "dailyMinTemperature=?, "
				+ "dailyHumidityHours=?, "
				+ "dailyMinTemperatureFlag=?, "
				+ "dailyHumidityHoursFlag=?, "
				+ "dailyMinTemperatureNearMissFlag=?, "
				+ "dailyHumidityHoursNearMissFlag=? "
				+ "WHERE DATE(time) = DATE_ADD(CURDATE(), INTERVAL ? DAY) AND location_id=?;");

			ips.setFloat(1, minTemperature);
			ips.setFloat(2, humidityHoursCount);
			ips.setNull(3, Types.FLOAT);
			ips.setNull(4, Types.FLOAT);
			ips.setNull(5, Types.FLOAT);
			ips.setNull(6, Types.FLOAT);

			if (minTemperature >= 10f)
				ips.setFloat(3, 1);
			else if (minTemperature >= 9f)
				ips.setFloat(5, 1);

			if (humidityHoursCount >= 6f)
				ips.setFloat(4, 1);
			else if (humidityHoursCount >= 5f)
				ips.setFloat(6, 1);


			ips.setInt(7, i);
			ips.setLong(8, locationId);

			ips.execute();

		}
	}

	private void runObservedCalculations()
		throws Exception
	{
		// Loop over the locations
		PreparedStatement s = c.prepareStatement("SELECT * FROM locations WHERE observed = TRUE;");
		ResultSet rs = s.executeQuery();

		while (rs.next())
		{
			long id = rs.getLong(1);
			String name = rs.getString(2);

			System.out.println("Running calculations for " + name);
			calculateObserved(id);

//			if (true)
//				System.exit(0);
		}

		rs.close();
		s.close();
	}

	private void calculateObserved(long locationId)
		throws Exception
	{
		for (int i = -14; i <= 10; i++)
		{
			Calendar cal = getCalendar(i);

			System.out.println("Checking day " + i);
			System.out.println(cal.getTime());
//			System.out.println(cal.getTimeInMillis());
//			System.out.println(cal.getTimeInMillis() + (24*60*60*1000));


			long lastTime = cal.getTimeInMillis();


//			ips = c.prepareStatement("SELECT * FROM observed WHERE DATE(time) = DATE_ADD(CURDATE(), INTERVAL ? DAY) AND location_id=?;");
			ips = c.prepareStatement("SELECT * FROM observed WHERE time >= FROM_UNIXTIME(?) and time < FROM_UNIXTIME(?) AND location_id=?;");
			// Midnight on the day (in seconds)
			ips.setLong(1, cal.getTimeInMillis() / 1000);
			// Midnight 24 hours later (in seconds)
			ips.setLong(2, cal.getTimeInMillis() / 1000 + (24*60*60));
			ips.setLong(3, locationId);

			// How many milliseconds was humidity above the target?
			float humidityCountMS = 0;
			// Minimum daily temperature?
			float minTemperature = Float.MAX_VALUE;

			ResultSet rs = ips.executeQuery();
			while (rs.next())
			{
				Timestamp ts = rs.getTimestamp(3);
				System.out.println("  time: " + ts);
				long timeSinceLastReading = ts.getTime() - lastTime;
//				System.out.println("  time since last reading " + timeSinceLastReading / 1000f / 60f);
				lastTime = ts.getTime();

				float temperature = rs.getFloat(4);
				float humidity = rs.getFloat(5);

				System.out.println("    " + temperature + "c, " + humidity + "%");

				// How many hours have a humidity value higher than 90%
				if (humidity >= 90f)
					humidityCountMS += timeSinceLastReading;

				// Track the minimum temperature for the day
				minTemperature = Math.min(minTemperature, temperature);
			}
			rs.close();

			// If lastTime never updated, the DB didn't return any results
			if (lastTime == cal.getTimeInMillis())
				continue;

			float humidityHoursCount = humidityCountMS / 1000f / 60f / 60f;

			System.out.println("humidityHoursCount=" + humidityHoursCount);
			System.out.println("minTemperature=" + minTemperature);

			// Now update the rows based on our calculations
			ips = c.prepareStatement("UPDATE observed SET "
				+ "dailyMinTemperature=?, "
				+ "dailyHumidityHours=?, "
				+ "dailyMinTemperatureFlag=?, "
				+ "dailyHumidityHoursFlag=?, "
				+ "dailyMinTemperatureNearMissFlag=?, "
				+ "dailyHumidityHoursNearMissFlag=? "
				+ "WHERE time >= FROM_UNIXTIME(?) and time < FROM_UNIXTIME(?) AND location_id=?");

			ips.setFloat(1, minTemperature);
			ips.setFloat(2, humidityHoursCount);
			ips.setNull(3, Types.FLOAT);
			ips.setNull(4, Types.FLOAT);
			ips.setNull(5, Types.FLOAT);
			ips.setNull(6, Types.FLOAT);

			if (minTemperature >= 10f)
				ips.setFloat(3, 1);
			else if (minTemperature >= 9f)
				ips.setFloat(5, 1);

			if (humidityHoursCount >= 6f)
				ips.setFloat(4, 1);
			else if (humidityHoursCount >= 5f)
				ips.setFloat(6, 1);


			// Midnight on the day (in seconds)
			ips.setLong(7, cal.getTimeInMillis() / 1000);
			// Midnight 24 hours later (in seconds)
			ips.setLong(8, cal.getTimeInMillis() / 1000 + (24*60*60));
			ips.setLong(9, locationId);

			ips.execute();
		}
	}

	private Calendar getCalendar(int offset)
	{
		Calendar cal = Calendar.getInstance();
		cal.setTimeZone(TimeZone.getTimeZone("Europe/London"));
		cal.add(Calendar.DATE, offset);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);

		return cal;
	}
}