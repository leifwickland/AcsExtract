import io.Source
import collection.Iterator
import collection.immutable.ListMap

object AcsExtract {
  implicit def stringToRicherString(s: String) = new RicherString(s)
  class RicherString(s: String) {
    def columnize(length: Int): String = columnize(length, ' ')
    def columnize(length: Int, pad: Char): String = s.substring(0,s.length.min(length)).padTo(length, pad)
    def trimToScreen: String = System.getenv("COLUMNS") match { 
      case null => s
      case columns: String => try {s.substring(0, s.length.min(columns.toInt)) } catch { case ex: Exception => s } 
    }
  }

  def main(args: Array[String]) {
    if (args.length == 0) {
      printUsage("You must specify at least one field or 'all'.")
      sys.exit(1)
    } 
    try {
      val (fieldsToExtract, input) = readArguments(args)
      parse(fieldsToExtract, input)
    } catch {
      case ex: Exception => printUsage(ex.toString); throw ex
    }
  }

  def readArguments(args: Array[String]): (Map[String, Option[String]], Source) = {
    var toExtract = ListMap[String, Option[String]]()
    var input = Source.fromInputStream(System.in)
    for (i <- 0 until args.length) {
      if (args(i) == "all") {
        toExtract = toExtract ++ fields.map((_._1 -> None))
      } else {
        fields.get(args(i)) match {
          case Some(f) => toExtract = (toExtract + (args(i) -> None))
          case None if (i == args.length - 1) => input = Source.fromFile(args(i))
          case None => throw new Exception("Unknown field: " + args(i))
        }
      }
    }
    (toExtract, input)
  }

  val fields = ListMap[String, (String, Map[String,String] => String)](
    ("d" -> ("timestamp", { a => a("datetime").columnize(13) })),
    ("D" -> ("human readable time", { a => new java.util.Date(a("datetime").toLong).toString.substring(11).columnize(8) } )),
    ("r" -> ("recorded by", { a => if (a("recordedby") == "server") "s" else if (a("recordedby") == "capture") "c" else "!"})),
    ("S" -> ("subject", { a => a.getOrElse("subject", "MISSING!").columnize(12) })),
    ("V" -> ("verb", { a => a.getOrElse("verb", "MISSING!").columnize(15) })),
    ("O" -> ("object", { a => a.getOrElse("object", "").columnize(20) })),
    ("u" -> ("url", { a => a.getOrElse("url", "MISSING!").columnize(100) })), 
    ("e" -> ("engagement ID", { a => a.getOrElse("engagementid", if (a("recordedby") == "capture") "MISSING!" else "").columnize(8) })),
    ("s" -> ("session ID", { a => a.getOrElse("sessionid", "MISSING!").columnize(8) })),
    ("b" -> ("billing ID", { a => a.getOrElse("billinggroupid", "MISSING!").columnize(8) })),
    ("i" -> ("instance ID", { a => a.getOrElse("instanceid", "MISSING!").columnize(22) })),
    ("f" -> ("product family", { a => a.getOrElse("productfamily", "!").substring(0, 1) })),
    ("p" -> ("product",  { a => a.getOrElse("product", "MISSING!").columnize(25) })),
    ("v" -> ("product version",  { a => a.getOrElse("productversion", "MISSING!").columnize(25) })),
    ("a" -> ("anonymized IP", { a => a.getOrElse("anonymizedip", "MISSING!").columnize(11) })),
    ("h" -> ("hashed IP", { a => a.getOrElse("hashedip", "MISSING!").columnize(32) })),
    ("A" -> ("alt session ID", { a => a.getOrElse("altsessionid", "MISSING!").columnize(32) })),
    ("U" -> ("user agent", { a => a.getOrElse("useragent", "MISSING!").columnize(140) }))
  )

  def printUsage(errorMessage: String = null) { 
    if (errorMessage != null) {
      println("Error: " + errorMessage)
      println("")
    }
    println("Usage: [space delimited list of fields|all] [file name]")
    println("")
    println("Fields:")

    fields.toSeq.sortBy(_._2._1).foreach{ f => println("    " + f._1 + ": " + f._2._1)}
    println("")
  }


  //def parse(sources: Seq[Source]): Unit = parse(sources.foldLeft[Iterator[String]](Iterator.empty)(_ ++ _.getLines))

  def parse(fieldsToExtract: Map[String, Option[String]], source: Source): Unit = parse(fieldsToExtract, source.getLines)

  def parse(fieldsToExtract: Map[String, Option[String]], lines: Iterator[String]): Unit = {
    lines.
      flatMap(lineToColumns).
      map{ action => fieldsToExtract.map{ f => fields(f._1)._2(action) }.mkString(" ") }.
      foreach(println)
  }

  def lineToColumns(line: String): Option[Map[String,String]] = {
    try {
      return Some(com.codahale.jerkson.Json.parse[Map[String,AnyRef]](line).map {case (k,v) => (k,v.toString) })
    }
    catch {
      case ex: Exception => {
        println("ERROR PARSING: " + line)
        None
      }
    }
  }
}
