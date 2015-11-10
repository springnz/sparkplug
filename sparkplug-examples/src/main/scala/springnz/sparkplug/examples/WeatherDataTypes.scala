package springnz.sparkplug.examples

object WeatherDataTypes {

  case class StationData(
    id: String,
    callSign: Option[String],
    countryCode: Option[String],
    elevation: Double,
    lat: Double,
    long: Double,
    name: String,
    stateCode: Option[String])

  case class RawWeatherData(
    wsid: String,
    year: Int,
    month: Int,
    day: Int,
    hour: Int,
    dewpoint: Double,
    oneHourPrecip: Double,
    pressure: Double,
    sixHourPrecip: Double,
    skyCondition: Int,
    skyConditionText: String,
    temperature: Double,
    windDirection: Int,
    windSpeed: Double)
}

