import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import com.google.gson.Gson
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.util
import twitter4j.TwitterFactory
import org.apache.spark.streaming.twitter._
import twitter4j.conf.ConfigurationBuilder
import twitter4j.Twitter

import org.apache.spark.sql.SparkSession


/**
 * Collect at least the specified number of tweets into json text files.
 */
object TwitterStream {
  private var numTweetsCollected = 0L
  var post = 0L
  var oldpost = 1L

  private var partNum = 0
  private var gson = new Gson()

  def main(args: Array[String]) {
    // Process program arguments and set properties
    if (args.length < 1) {
      System.err.println("Usage: " + this.getClass.getSimpleName +
        "<outputDirectory> <intervalInSeconds> <filter hashtags>")
      System.exit(1)
    }


   // val Array(outputDirectory, intervalInSeconds) = args.take(2)

    val outputDirectory = args(0)
    val intervalInSeconds = args(1)

    val filters = args.takeRight(args.length - 2)

    val outputDir = new File(outputDirectory.toString)
    if (outputDir.exists()) {
      System.err.println("%s already exists ".format(outputDirectory))
      //System.exit(1)
    }
    else {
	  System.err.println("%s ".format(outputDirectory))
    	outputDir.mkdirs()
    }

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(intervalInSeconds.toInt))
	
	// Creating Dstream of Tweets
    val tweetStream = TwitterUtils.createStream(ssc, None, filters)
    println("tweetStream.getClass %s".format(tweetStream.getClass))

    val hashTags = tweetStream.flatMap(status => status.getText.split(" ").filter(_.contains("#")))
    println("hashTags.getClass %s".format(hashTags.getClass))
	
    val tweetGson = tweetStream.map(gson.toJson(_))
	var i = 0
    tweetGson.foreachRDD(rdd => {

	i += 1 

	println("**************Micro Batch # "+ i + "**************")


			val spark = SparkSession
			  .builder()
			  .appName("TwitterStreamAnalysis")
			  .config("spark.some.config.option", "some-value")
			  .getOrCreate()

			//rdd.foreach(println)

			// For implicit conversions like converting RDDs to DataFrames
			import spark.implicits._

			// Converting RDD[String] to DataFrame
			val wordsDataFrame = spark.read.json(rdd)

			  // Create a temporary view
			wordsDataFrame.createOrReplaceTempView("words")
			wordsDataFrame.write.mode("append").json(outputDirectory+"/abc")

			if(!(wordsDataFrame.head(1).isEmpty))	
			{
			
					/* Counting tweet by location starts*/

					println("Starting tweet processing")

					  val newDF1 = wordsDataFrame.select($"user.name".as("name"), $"text", $"user.location".as("location"))
					  newDF1.createOrReplaceTempView("newDF1")

					  //show the tweets
					  println("******Start Printing tweets for Micro Batch#"+i+"******")
					  println("******                   ******")

					  newDF1.show(false)

					  println("******                   ******")
					  println("******End Printing tweetsMicro Batch#"+i+"******")

					  val wordCountsDataFrame1 = spark.sql("select count(*), count(distinct name), count(distinct location) from newDF1")

					  wordCountsDataFrame1.foreach( row => {

							  var count = row(0)
							  var name = row(1)
							  var location = row(2)				

							  println("With these hashtags "+filters+" "
									  + count +" unique tweet(s) from " + name + " unique user(s) from "+ location +" unique locations in last "+intervalInSeconds+" second(s)")

						})

					println("Ending tweet processing")

					/* Counting tweet by location Ends */


			}
     
    })

    ssc.start()
    ssc.awaitTermination()
  }
} 
