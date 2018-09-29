
import org.apache.spark.rdd.RDD

object TweetAnalytics {

  def extractTweets(rdd: RDD[String]):RDD[(String,String,String,String,String)] = {
    val tweets = rdd.map(t => t.split("\",\""))
      .filter(fields => fields.length==5)
      .map(fields => (fields(0), fields(1), fields(2), fields(3), fields(4)))
    tweets
  }

  def cleanTweets(rdd:RDD[(String,String,String,String,String)]):RDD[(String,String,String,String,String)]  = {
    val cleanTweets = rdd.map(elem => {
      (if(elem._1.startsWith("{\"idTweet\":\"")) elem._1.replace("{\"idTweet\":\"", "") else elem._1.replace("{\"", ""),
        if(elem._2.startsWith("text\":\"")) elem._2.replace("text\":\"","") else elem._2,
        if(elem._3.startsWith("idAuthor\":\"")) elem._3.replace("idAuthor\":\"","") else elem._3,
        if(elem._4.startsWith("idOriginal\":\"")) elem._4.replace("idOriginal\":\"","") else elem._4,
        if(elem._5.startsWith("origAuthor\":\"")) elem._5.replace("origAuthor\":\"","").replace("\"}","") else elem._5.replace("\"}",""))
    })
    cleanTweets
  }

  def cleanTweets2(rdd:RDD[(String,String,String,String,String)]):RDD[(String,String,String,String,String)]  = {
    val cleanTweets = rdd.map(elem => {
      (if (elem._1.startsWith("{\"idTweet\":\"")) elem._1.replace("{\"idTweet\":\"", "") else elem._1.replace("{\"", ""),
        if (elem._2.startsWith("text\":\"")) elem._2.replace("text\":\"", "") else elem._2,
        if (elem._3.startsWith("idAuthor\":\"")) elem._3.replace("idAuthor\":\"", "") else elem._3,
        if (elem._4.startsWith("idOriginal\":\"")) elem._4.replace("idOriginal\":\"", "") else elem._4,
        if (elem._5.startsWith("origAuthor\":\"")) elem._5.replace("origAuthor\":\"", "").replace("\"}", "") else elem._5.replace("\"}", ""))
    })
    cleanTweets
  }

  def convertTweets(rdd:RDD[(String,String,String,String,String)]):RDD[(Long,String,Long,Long,Long)]  = {
    val longTweets = rdd.map(elem => {(elem._1.toLong, elem._2, elem._3.toLong, elem._4.toLong, elem._5.toLong)
    })
    longTweets
  }

  def tweetsFromUser(rdd:RDD[(String,String,String,String,String)],user:String):RDD[(Long,String,Long,Long,Long)] = {
    val tweetsFromUser = rdd.filter(t => t._3.equals(user))
    convertTweets(tweetsFromUser)
  }

  def countWords(rdd:RDD[(Long,String,Long,Long,Long)]):RDD[(String,Int)] = {
    val countWords = rdd.flatMap(line => line._2.split(" +")).map(w => (w,1)).reduceByKey(_ + _)
    countWords
  }

  def wordsStartWith(rdd:RDD[(String,Int)], str:String):RDD[(String,Int)] = {
    val words = rdd.sortBy(-_._2).filter(w => w._1.startsWith(str))
    words
  }

  def wordsNotContainedIn(rdd:RDD[(String,Int)], list:List[String], t:Integer):Array[(String,Int)] = {
    val words = rdd.sortBy(-_._2).take(t).filter(t => !list.contains(t._1))
    words
  }

  def wordsContainedIn(rdd:RDD[(String,Int)], list:List[String], t:Integer):Array[(String,Int)] = {
    val words = rdd.sortBy(-_._2).take(t).filter(t => list.contains(t._1))
    words
  }


}
