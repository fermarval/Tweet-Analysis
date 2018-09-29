import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions._

val spark = SparkSession.builder()
  .appName("Data Trump")       //Name for the session
  .master("local[2]")                //Path and number of cores to be used
  .getOrCreate()

//Optional: set logging level if log4j not configured
Logger.getRootLogger.setLevel(Level.ERROR)

//RDD
val filePath = "./src/resources/datatrump.txt"
val testRDD = spark.sparkContext.textFile(filePath)

//Extract id, text, idAuthor, idOriginal and oriAuthor
val tweets = TweetAnalytics.extractTweets(testRDD)
tweets.collect.foreach(elem => println("[" + elem._1 + ", " + elem._2 + ", " + elem._3 + ", " + elem._4 + ", " + elem._5 + "]" ))

//Clean and fix elements
val cleanTweets = TweetAnalytics.cleanTweets(tweets)
cleanTweets.collect.foreach(elem => println("[" + elem._1 + ", " + elem._2 + ", " + elem._3 + ", " + elem._4 + ", " + elem._5 + "]" ))

//Convert RDD[(String, String, String)] in RDD[(Long, Long, Long)]
val longTweets = TweetAnalytics.convertTweets(cleanTweets)
longTweets.collect.foreach(elem => println("[" + elem._1 + ", " + elem._2 + ", " + elem._3 + ", " + elem._4 + ", " + elem._5 + "]" ))


val stopWords = List( "a", "about", "above", "after", "all", "am", "an", "and", "any", "are", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "could", "did", "do", "does", "doing", "down", "during", "each", "few", "for", "from", "further", "had", "has", "have", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "I", "I'd", "I'll", "I'm", "I've", "if", "in", "into", "is", "it", "it's", "its", "itself", "let's", "me", "more", "most", "my", "myself", "nor", "of", "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "she", "she'd", "she'll", "she's", "should", "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "we", "we'd", "we'll", "we're", "we've", "were", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with", "would", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves" )
val cities = List("video","York","#York","NY","NYC","#NY","#NYC","Angeles","#Angeles","LA","#LA","Chicago","#Chicago","Houston","#Houston","Phoenix","#Phoenix","Philadelphia","#Philadelphia","Dallas","#Dallas","Austin","#Austin","Jacksonville","#Jacksonville","Francisco","#Francisco","SF","Columbus","#Columbus","Indianapolis","#Indianapolis","Charlotte","#Charlotte","Seattle","#Seattle","Denver","#Denver","Washington","#Washington")

//Count Words
val countWords = TweetAnalytics.countWords(longTweets)
//println("Words count: " + countWords.count())

//List of words most used (without clean stop words):
countWords.sortBy(-_._2).take(40).foreach(t => println("Word: " + t._1 + " \tOcurrences: "+ t._2))

//List of words most used:
countWords.sortBy(-_._2).take(40).filter(t => !stopWords.contains(t._1)).foreach(t => println("Word: " + t._1 + " \tOcurrences: "+ t._2))

//List of Hashtags (#) most used:
TweetAnalytics.wordsStartWith(countWords,"#").take(10).foreach(t => println("Word: " + t._1 + " \tOcurrences: "+ t._2))

//List of users (@) most mentioned:
TweetAnalytics.wordsStartWith(countWords,"#").take(11).foreach(t => println("Word: " + t._1 + " \tOcurrences: "+ t._2))

//List user with most tweets:
val countUserMostTweets = longTweets.map(line => line._3).map(u => (u,1)).reduceByKey(_ + _)
countUserMostTweets.sortBy(-_._2).take(10).foreach(u => println("User: " + u._1 + " \tOcurrences: "+ u._2))

//List user with most RT:
val countUserMostRT = longTweets.map(line => line._5).map(u => (u,1)).reduceByKey(_ + _)
countUserMostRT.sortBy(-_._2).filter(u => !u.equals(0)).take(11).foreach(u => println("User: " + u._1 + " \tOcurrences: "+ u._2))

//Count Words
val countWordsUser = longTweets.filter(line => line._3.equals(25073877)).flatMap(line => line._2.split(" ")).map(w => (w,1)).reduceByKey(_ + _)
//Filter tweets from a user
TweetAnalytics.wordsContainedIn(longTweets,stopWords,10).foreach(t => println("Word: " + t._1 + " \tOcurrences: "+ t._2))