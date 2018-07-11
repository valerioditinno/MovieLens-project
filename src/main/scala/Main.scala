package main.scala

import org.apache.spark.{SparkConf, SparkContext}
import MyUtils._

object Main {

  def main(args: Array[String]) {

    val runQuery1:Boolean = false
    val runQuery2:Boolean = false
    val runQuery3:Boolean = true

    // Spark configuration
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("MovieLens project")
    val sc = new SparkContext(conf)

    // DATA preprocessing: this dataset describes 5-star rating activity from [MovieLens]

    // Load ratings datatset from .csv file into a Spark RDD  (format: userId,movieId,rating,timestamp)
    val csv1 = sc.textFile("src/main/resources/dataset/ratings.csv")    //partitioning ???
    // split / clean data
    val headerAndRows1 = csv1.map(line => line.split(",").map(_.trim))
    // get header
    val header1 = headerAndRows1.first
    // filter out header (eh. just check if the first val matches the first header name)
    val data1 = headerAndRows1.filter(_(0) != header1(0))
    // splits to map (header/value pairs)
    val structuredData1 = data1.map(splits => header1.zip(splits).toMap)
    val mappedData1 = structuredData1.map(map => (map("movieId"),(map("userId"),map("rating"),map("timestamp"))))

    // Load movies datatset from .csv file into a Spark RDD  (format: movieId,title,genres)
    val csv2 = sc.textFile("src/main/resources/dataset/movies.csv")
    // split / clean data
    val headerAndRows2 = csv2.map(line => line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)").map(_.trim))  // lower case ??
    // get header
    val header2 = headerAndRows2.first
    // filter out header (eh. just check if the first val matches the first header name)
    val data2 = headerAndRows2.filter(_(0) != header2(0))  //.map(_.toString().toLowerCase)
    // splits to map (header/value pairs)
    val structuredData2 = data2.map(splits => header2.zip(splits).toMap)
    val mappedData2 = structuredData2.map(map => (map("movieId"),(map("title"),map("genres"))))

    val joinedData = mappedData2.join(mappedData1)  // (movieId,((title, genres),(userId,rating,timestamp)))

    ////////////////////////////////////////////////////////////////////////////////////
    // QUERY 1: Find the movies with a rating >= 4.0 and rated after January 1st  2000

    /*
    // Query 1 solved without lambda function

    type RateCollector = (Int, (Double, Int))
    type MovieRates = (String, (Int, (Double, Int)))

    val preprocessedData = joinedData.map(t => (t._1, (t._2._2._2.toDouble, t._2._2._3.toInt))) // (movieId, (rating, timestamp))

    val createRateCombiner = (rate: (Double, Int)) => (1, rate)

    val rateCombiner = (collector: RateCollector, rate: (Double, Int)) => {
      val (numberRates, (totalRate, timestamp)) = collector
      (numberRates + 1, (totalRate + rate._1, if(timestamp >= rate._2) rate._2 else timestamp))
    }

    val rateMerger = (collector1: RateCollector, collector2: RateCollector) => {
      val (numRates1, (totalRate1, timestamp1)) = collector1
      val (numRates2, (totalRate2, timestamp2)) = collector2
      (numRates1 + numRates2, (totalRate1 + totalRate2,  if(timestamp1 >= timestamp2) timestamp1 else timestamp2))
    }
    val rates = preprocessedData.combineByKey(createRateCombiner, rateCombiner, rateMerger)

    val averagingFunction = (movieRate: MovieRates) => {
      val (name, (numberRates, (totalRate, timestamp))) = movieRate
      (name, ((totalRate / numberRates), timestamp))
    }

    val averageRates = rates.collectAsMap().map(averagingFunction)

    val query1Result = averageRates.filter(t => t._2._1 >= 4.0 && t._2._2 >= refData)

    println("Average movie rate using CombingByKey")
    query1Result.foreach((ps) => {
      val(id,(average, timestamp)) = ps
      println("movieId: " +id+ ", average rate: " + average + ", timestamp: " + timestamp)
    })*/
    if(runQuery1){
      val preprocessedData = joinedData.map(t => (t._1, (t._2._2._2.toDouble, t._2._2._3.toLong))) // (movieId, (rating, timestamp))

      val keyedData = preprocessedData.combineByKey(
        (v: (Double, Long)) => (1, v),
        (acc: (Int, (Double, Long)), v: (Double, Long)) => (acc._1 + 1, (acc._2._1 + v._1, if (v._2 >= acc._2._2) v._2 else acc._2._2)),
        (acc1: (Int, (Double, Long)), acc2: (Int, (Double, Long))) => (acc1._1 + acc2._1, (acc1._2._1 + acc2._2._1, if (acc1._2._2 >= acc2._2._2) acc1._2._2 else acc2._2._2)))
        .map { case (key, (num, (sum, timestamp))) => if (num >= 50) (key, (sum / num.toFloat, timestamp)) else (key, (0.toDouble, timestamp)) }

      val query1Result = keyedData.filter(t => t._2._1 >= 4.0 && t._2._2 >= refData)
      query1Result.collectAsMap().map(println(_))
    }

    ////////////////////////////////////////////////////////////////////////////////////
    // QUERY 2: Find the average rating and the standard deviation for each movie genre  // TODO fix this query ...  it dosen't work

    // .split("\\|")

    if(runQuery2) {
      val preprocessedData = joinedData.map(t => (t._2._1._2, t._2._2._2.toDouble)) // (genres, rating)
        .filter(t => t._1 != "(no genres listed)")

      val query2Result = preprocessedData.combineByKey(
        (v: (Double)) => (1, 0.toDouble, v),
        (acc: (Int, Double, Double), v: (Double)) => (acc._1 + 1, acc._2 + v * v, acc._3 + v),
        (acc1: (Int, Double, Double), acc2: (Int, Double, Double)) => (acc1._1 + acc2._1, acc1._2 + acc2._2, acc1._3 + acc2._3))
        .map { case (key, (n, sumsq, sum)) => (key, if (n > 1) (sum / n.toFloat, math.sqrt((sumsq - sum * sum / n.toFloat) / (n - 1).toFloat)) else (sum, 0.toFloat)) } // handle NaN 0/0

      query2Result.collectAsMap().map(println(_))
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // QUERY 3: Find the top-10 highest rate movies in the last dataset year and compare their position in the rank
    // with respect to that achieved in the previuos year

    if(runQuery3) {
      val preprocessedData = joinedData.map(t => (t._1, (t._2._2._2.toDouble, t._2._2._3.toLong))) // (movieId, (rating, timestamp))

      val lastYear = preprocessedData.filter(t => t._2._2 >= lastYearStart && t._2._2 <= lastYearEnd)
      val previousYear = preprocessedData.filter(t => t._2._2 >= previousYearStart && t._2._2 <= previousYearEnd)

      val lastYearResult = lastYear.combineByKey(
        (v: (Double, Long)) => (1, v),
        (acc: (Int, (Double, Long)), v: (Double, Long)) => (acc._1 + 1, (acc._2._1 + v._1, if (v._2 >= acc._2._2) v._2 else acc._2._2)),
        (acc1: (Int, (Double, Long)), acc2: (Int, (Double, Long))) => (acc1._1 + acc2._1, (acc1._2._1 + acc2._2._1, if (acc1._2._2 >= acc2._2._2) acc1._2._2 else acc2._2._2)))
        .map { case (key, (num, (sum, timestamp))) => if (num >= 50) (key, (sum / num.toFloat, timestamp)) else (key, (0.toDouble, timestamp)) }

      val previousYearResult = previousYear.combineByKey(
        (v: (Double, Long)) => (1, v),
        (acc: (Int, (Double, Long)), v: (Double, Long)) => (acc._1 + 1, (acc._2._1 + v._1, if (v._2 >= acc._2._2) v._2 else acc._2._2)),
        (acc1: (Int, (Double, Long)), acc2: (Int, (Double, Long))) => (acc1._1 + acc2._1, (acc1._2._1 + acc2._2._1, if (acc1._2._2 >= acc2._2._2) acc1._2._2 else acc2._2._2)))
        .map { case (key, (num, (sum, timestamp))) => if (num >= 50) (key, (sum / num.toFloat, timestamp)) else (key, (0.toDouble, timestamp)) }

      val lastYearRank = lastYearResult.takeOrdered(10)(Ordering[Double].reverse.on(x => x._2._1))

      val previuosYearRank = previousYearResult.takeOrdered(10)(Ordering[Double].reverse.on(x => x._2._1))

      for (x <- lastYearRank.zip(previuosYearRank)) println(x)
    }

  }

}