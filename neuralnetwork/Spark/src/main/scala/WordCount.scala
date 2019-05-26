import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark._
import java.io._

object WordCount {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val regex = """[^a-zA-Z ]""".r

    val lines = sc.textFile("in/word_count.txt")
    val words = lines.flatMap(line=>regex.replaceAllIn(line, "").toLowerCase().split(" "))
    val wordCounts = words.countByValue()

    val pw = new PrintWriter(new File("output/text1.txt" ))
    for ((word, count) <- wordCounts) {
      pw.write(word + " : " + count)
      pw.write("\n")
    }
    pw.close()

    val text2 = sc.textFile("in/lorem_ipsum.txt")
    val words2 = text2.flatMap(line=>regex.replaceAllIn(line, "").toLowerCase().split(" "))
    val intersection = words2.countByValue().filter(word => wordCounts.contains(word._1))

    val pw1 = new PrintWriter(new File("output/text2.txt" ))
    for ((word, count) <- intersection) {
      pw1.write(word + " : " + count)
      pw1.write("\n")
    }
    pw1.close()
    
  }
}
