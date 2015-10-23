package springnz.sparkplug.util

import java.io._

import scala.reflect.ClassTag

object SerializeUtils {
  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }

  def deserialize[T: ClassTag](bytes: Array[Byte]): T = {
    deserialize[T](bytes, getClass.getClassLoader)
  }

  def deserialize[T](bytes: Array[Byte], jarFile: String): T = {
    val parentClassLoader = this.getClass.getClassLoader
    val jarURLArray = Array(new File(jarFile).toURI.toURL)
    val classLoader = new java.net.URLClassLoader(jarURLArray, parentClassLoader)
    deserialize[T](bytes, classLoader)
  }

  def deserialize[T](bytes: Array[Byte], loader: ClassLoader): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis) {
      override def resolveClass(desc: ObjectStreamClass): Class[_] =
        Class.forName(desc.getName, false, loader)
    }
    ois.readObject.asInstanceOf[T]
  }

}
