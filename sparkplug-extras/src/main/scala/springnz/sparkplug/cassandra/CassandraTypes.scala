package springnz.sparkplug.cassandra

import scala.util.Try

object CassandraTypes {

  case class KeySpace(name: String) extends AnyVal {
    override def toString = name
  }

  case class Table(name: String) extends AnyVal {
    override def toString = name
  }

  type CassandraInsertResult = Try[Unit]

}
