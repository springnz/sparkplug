import java.io.{ PrintWriter, File }

object PrintToFile {
  val list = List("a", "b", "c")

  def printToFile(f: File)(op: PrintWriter ⇒ Unit) {
    val p = new java.io.PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }

  printToFile(new File("test.txt")) {
    p ⇒
      list.foreach {
        line ⇒ p.println(line)
      }
  }
}
