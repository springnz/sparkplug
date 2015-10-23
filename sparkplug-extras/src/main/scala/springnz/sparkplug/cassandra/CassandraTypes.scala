package springnz.sparkplug.cassandra

object CassandraTypes {

  case class KeySpace(keySpace: String) extends AnyVal {
    override def toString = keySpace
  }

}
