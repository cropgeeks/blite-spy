package jhi.blitespy;

import java.sql.*;

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
		calculator.runCalculations();
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

	private void runCalculations()
		throws Exception
	{
		// Loop over the locations
		PreparedStatement s = c.prepareStatement("SELECT * FROM locations;");
		ResultSet rs = s.executeQuery();

		while (rs.next())
		{
			long id = rs.getLong(1);
			String name = rs.getString(2);
			float lat = rs.getFloat(3);
			float lon = rs.getFloat(4);
			float alt = rs.getFloat(5);

			System.out.println("Running calculations for " + name);
			calculate(id, lat, lon);

//			if (true)
//				System.exit(0);
		}

		rs.close();
		s.close();
	}

	private void calculate(long id, float lat, float lon)
		throws Exception
	{
		// Process up to 10 days' worth of forecast information
		for (int i = 0; i < 10; i++)
		{
			System.out.println("Checking day " + i);

			ips = c.prepareStatement("SELECT * FROM forecasts WHERE DATE(time) = DATE_ADD(CURDATE(), INTERVAL ? DAY) AND location_id=?;");
			ips.setInt(1, i);
			ips.setLong(2, id);

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
			ips.setLong(8, id);

			ips.execute();

		}
	}
}