package springnz.sparkplug.examples

import org.json4s.DefaultFormats

object WorldDataTypes {
  case class Country(
    Code: String,
    Name: String,
    Continent: String,
    Region: String,
    SurfaceArea: Double,
    IndepYear: Option[Long],
    Population: Long,
    LifeExpectancy: Option[Double],
    GNP: Option[Double],
    GNPOld: Option[Double],
    LocalName: String,
    GovernmentForm: String,
    HeadOfState: Option[String],
    Capital: Option[Long],
    Code2: String)
}
