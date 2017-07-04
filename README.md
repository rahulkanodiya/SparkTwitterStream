# Spark Twitter Stream Application

A sample Spark application to fetch twitter stream based on a few specified tags.

To run, open a terminal and execute below commands:
$SPARK_HOME/bin/spark-submit --master local[2] --packages org.apache.bahir:spark-streaming-twitter_2.11:2.0.1 
--class TwitterStream <Location Where you unzipped the tar>/TwitterStream/target/scala-2.11/twitter-stream_2.11-1.0.jar
<Output Path to save tweets> 1 <"sample hashtags" "BigData" "Apache Spark" "Spark" "Hadoop">

