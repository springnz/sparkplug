package springnz.sparkplug.examples

import springnz.sparkplug.cassandra.CassandraTypes.{ Table, KeySpace }

object CassandraConstants {
  val projectFolder = "sparkplug-core"
  val weatherKeySpace = KeySpace("isd_weather_data")
  val rawWeatherData = Table("raw_weather_data")
  val weatherStations = Table("weather_station")
}

