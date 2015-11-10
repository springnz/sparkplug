//package springnz.sparkplug.core
//
//import springnz.util.Logging
//
//object SparkConverters extends Logging {
//
//  implicit def map2Properties(map: Map[String, String]): java.util.Properties = {
//    (new java.util.Properties /: map) { case (props, (k, v)) ⇒ props.put(k, v); props }
//  }
//
//}
//

// TODO: remove - should be using Pimpers from util-lib