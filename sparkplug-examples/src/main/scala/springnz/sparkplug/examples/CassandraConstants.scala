package springnz.sparkplug.examples

import springnz.sparkplug.cassandra.CassandraTypes.KeySpace

object CassandraConstants {
  val projectFolder = "sparkplug-core"
  val weatherKeySpace = KeySpace("isd_weather_data")
  val rawWeatherData = "raw_weather_data"
  val weatherStations = "weather_station"
}

