import org.apache.spark.{SparkConf, SparkContext}

object WordCount1 {
  def main(args: Array[String]): Unit =
  {
    System.setProperty("hadoop.home.dir", "C:\\winutils\\") // Нужно для работы Spark

    val conf = new SparkConf().setAppName("Test").setMaster("local")
    val sc = new SparkContext(conf)

    //Считам тексты
    val text1 = sc.textFile("C:\\Users\\Максим\\ml-homework\\task5-spark\\data\\random_text_1.txt")
    val text2 = sc.textFile("C:\\Users\\Максим\\ml-homework\\task5-spark\\data\\random_text_2.txt")

    //Регуляркой уберем все, кроме букв
    val regex = """[^a-zA-Z ]""".r

    //Разобьем тексты на слова, переведем все символы в нижний регистр
    val words_text1 = text1.flatMap(line=>regex.replaceAllIn(line, "").toLowerCase().split(" ")).collect()
    val words_text2 = text2.flatMap(line=>regex.replaceAllIn(line, "").toLowerCase().split(" "))

    //Считаем количество вхождений каждого слова для text2
    val text1_all_words_count = words_text2.map(word=>(word,1)).reduceByKey(_+_).collect()

    //Считаем количество вхождений каждого слова для text2, который есть в text1
    val words_text2_from_text1 = text2.flatMap(line=>regex.replaceAllIn(line, "").toLowerCase().split(" "))
      .filter(word=>(words_text1.contains(word))).map(word=>(word,1)).reduceByKey(_+_).collect()

    //Печатаем результаты
    println("WordCount text2:")
    text1_all_words_count.foreach(println)
    println("WordCount text2 that are in text1:")
    words_text2_from_text1.foreach(println)
  }
}
