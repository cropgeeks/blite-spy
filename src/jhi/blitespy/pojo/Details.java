package jhi.blitespy.pojo;

import com.google.gson.annotations.*;

public class Details
{
	@SerializedName(value = "air_pressure_at_sea_level")
	public float airPressureAtSeaLevel;

	@SerializedName(value = "air_temperature")
	public float airTemperature;

	@SerializedName(value = "cloud_area_fraction")
	public float cloudAreaFraction;

	@SerializedName(value = "relative_humidity")
	public float relativeHumidity;

	@SerializedName(value = "wind_from_direction")
	public float windFromDirection;

	@SerializedName(value = "wind_speed")
	public float windSpeed;

	@SerializedName(value = "precipitation_amount")
	public float precipitationAmount;


	public float getAirPressureAtSeaLevel() {
		return airPressureAtSeaLevel; }

	public float getAirTemperature() {
		return airTemperature; }

	public float getCloudAreaFraction() {
		return cloudAreaFraction; }

	public float getRelativeHumidity() {
		return relativeHumidity; }

	public float getWindFromDirection() {
		return windFromDirection; }

	public float getWindSpeed() {
		return windSpeed; }

	public float getPrecipitationAmount() {
		return precipitationAmount; }
}