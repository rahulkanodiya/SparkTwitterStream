name := "Twitter-Stream"
 
version := "1.0"
 
scalaVersion := "2.11.6"
 
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.0.1",
    "org.apache.spark" %% "spark-streaming" % "2.0.1",
    "org.apache.spark" %% "spark-sql" % "2.0.1",
    "com.google.code.gson" % "gson" % "2.4",
//    "org.apache.spark" %% "spark-streaming-twitter" % "1.2.0"
//  "org.apache.spark" %% "spark-streaming" % "2.0.1",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.1"
)
