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

//Tweets from a user
val tweetsFromUser = TweetAnalytics.tweetsFromUser(cleanTweets,"716424335427153920")

//Number of tweet from a user:
tweetsFromUser.count()
tweetsFromUser.collect.foreach(elem => println("[" + elem._2+"]" ))

val cities = List("Aberdeen", "Abilene", "Akron", "Albany", "Albuquerque", "Alexandria", "Allentown", "Amarillo", "Anaheim", "Anchorage", "Ann Arbor", "Antioch", "Appleton", "Arlington", "Arvada", "Asheville", "Athens", "Atlanta", "Atlantic", "Augusta", "Aurora", "Austin", "Bakersfield", "Baltimore", "Barnstable", "Baton Rouge", "Beaumont", "Bellevue", "Berkeley", "Bethlehem", "Billings", "Birmingham", "Bloomington", "Boise", "Bonita", "Boston", "Boulder", "Bradenton", "Bremerton", "Bridgeport", "Brighton", "Brownsville", "Bryan", "Buffalo", "Burbank", "Burlington", "Cambridge", "Canton", "Cape Coral", "Carrollton", "Cary", "Cathedral City", "Cedar Rapids", "Champaign", "Chandler", "Charleston", "Charlotte", "Chattanooga", "Chesapeake", "Chicago", "Chula", "Cincinnati", "Clarke", "Clarksville", "Clearwater", "Cleveland", "College Station", "Colorado Springs", "Columbia", "Columbus", "Concord", "Coral Springs", "Corona", "Corpus Christi", "Costa Mesa", "Dallas", "Daly City", "Danbury", "Davenport", "Davidson County", "Dayton", "Daytona", "Deltona", "Denton", "Denver", "Moines", "Detroit", "Downey", "Duluth", "Durham", "Monte", "Paso", "Elizabeth", "Elk", "Elkhart", "Erie", "Escondido", "Eugene", "Evansville", "Fairfield", "Fargo", "Fayetteville", "Fitchburg", "Flint", "Fontana", "Fort Collins", "Fort Lauderdale", "Smith", "Walton", "Wayne", "Worth", "Frederick", "Fremont", "Fresno", "Fullerton", "Gainesville", "Garden", "Garland", "Gastonia", "Gilbert", "Glendale", "Prairie", "Rapids", "Grayslake", "GreenBay", "Greensboro", "Greenville", "Gulfport-Biloxi", "Hagerstown", "Hampton", "Harlingen", "Harrisburg", "Hartford", "Havre de Grace", "Hayward", "Hemet", "Henderson", "Hesperia", "Hialeah", "Hickory", "Hollywood", "Honolulu", "Houma", "Houston", "Howell", "Huntington", "Huntington", "Huntsville", "Independence", "Indianapolis", "Inglewood", "Irvine", "Irving", "Jackson", "Jacksonville", "Jefferson", "Jersey", "Johnson", "Joliet", "Kailua", "Kalamazoo", "Kaneohe", "Kansas", "Kennewick", "Kenosha", "Killeen", "Kissimmee", "Knoxville", "Lacey", "Lafayette", "Charles", "Lakeland", "Lakewood", "Lancaster", "Lansing", "Laredo", "Cruces", "Layton", "Leominster", "Lewisville", "Lexington", "Lincoln", "Rock", "Long", "Lorain", "Angeles", "Louisville", "Lowell", "Lubbock", "Macon", "Madison", "Manchester", "Marina", "Marysville", "McAllen", "McHenry", "Medford", "Melbourne", "Memphis", "Merced", "Mesa", "Mesquite", "Miami", "Milwaukee", "Minneapolis", "Miramar", "Viejo", "Mobile", "Modesto", "Monroe", "Monterey", "Montgomery", "Moreno", "Murfreesboro", "Murrieta", "Muskegon", "Myrtle Beach", "Naperville", "Naples", "Nashua", "Nashville", "Bedford", "Haven", "London", "Orleans", "York", "Newark", "Newburgh", "Newport", "Norfolk", "Normal", "Norman", "Charleston", "Vegas", "North Port", "Norwalk", "Norwich", "Oakland", "Ocala", "Oceanside", "Odessa", "Ogden", "Oklahoma", "Olathe", "Olympia", "Omaha", "Ontario", "Orange", "Orem", "Orlando", "Overland", "Oxnard", "Palm Bay", "Palm Springs", "Palmdale", "Panama City", "Pasadena", "Paterson", "Pembroke Pines", "Pensacola", "Peoria", "Philadelphia", "Phoenix", "Pittsburgh", "Plano", "Pomona", "Pompano Beach", "Arthur", "Orange", "Port Saint Lucie", "Port St. Lucie", "Portland", "Portsmouth", "Poughkeepsie", "Providence", "Provo", "Pueblo", "Punta Gorda", "Racine", "Raleigh", "Cucamonga", "Reading", "Redding", "Reno", "Richland", "Richmond", "Richmond", "Riverside", "Roanoke", "Rochester", "Rockford", "Roseville", "Sacramento", "Saginaw", "Louis", "Saint Paul", "Saint Petersburg", "Salem", "Salinas", "Salt", "San Antonio", "San Bernardino", "San Buenaventura", "San Diego", "San Francisco", "San Jose", "Santa Ana", "Santa Barbara", "Santa Clara", "Santa Clarita", "Cruz", "Maria", "Rosa", "Sarasota", "Savannah", "Scottsdale", "Scranton", "Seaside", "Seattle", "Sebastian", "Shreveport", "Simi", "Sioux" , "Bend", "Lyon", "Spartanburg", "Spokane", "Springdale", "Springfield", "Louis", "Paul", "Petersburg", "Stamford", "Sterling Heights", "Stockton", "Sunnyvale", "Syracuse", "Tacoma", "Tallahassee", "Tampa", "Temecula", "Tempe", "Thornton", "Thousand", "Toledo", "Topeka", "Torrance", "Trenton", "Tucson", "Tulsa", "Tuscaloosa", "Tyler", "Utica", "Vallejo", "Vancouver", "Vero", "Victorville", "Virginia", "Visalia", "Waco", "Warren", "Washington", "Waterbury", "Waterloo", "Covina", "Westminster", "Wichita", "Wilmington", "Winston", "Haven", "Worcester", "Yakima", "Yonkers", "Youngstown")
val countries = List("America","Afghanistan", "Albania", "Algeria", "Andorra", "Angola", "Anguilla", "Antarctica", "Barbuda", "Argentina", "Armenia", "Aruba", "Australia", "Austria", "Azerbaijan", "Bahamas", "Bahrain", "Bangladesh", "Barbados", "Belarus", "Belgium", "Belize", "Benin", "Bermuda", "Bhutan", "Bolivia", "Bosnia", "Botswana", "Brazil", "Darussalam", "Bulgaria", "Burkina", "Burundi", "Cambodia", "Cameroon", "Canada", "Cape", "Cayman", "Central", "Chad", "Chile", "China", "Cocos", "Colombia", "Comoros", "Congo", "Cook", "Rica", "Cote d'Ivoire", "Croatia", "Cuba", "Cyprus", "Czech Republic", "Denmark", "Djibouti", "Dominica", "Dominican", "East Timor", "Ecuador", "Egypt", "Salvador", "Guinea", "Eritrea", "Estonia", "Ethiopia", "Falkland Islands (Malvinas)", "Faroe Islands", "Fiji", "Finland", "France", "French", "Gabon", "Gambia", "Georgia", "Germany", "Ghana", "Gibraltar", "Greece", "Greenland", "Grenada", "Guadeloupe", "Guam", "Guatemala", "Guinea", "Guinea-Bissau", "Guyana", "Haiti", "Honduras", "Hong", "Hungary", "Iceland", "India", "Indonesia", "Iraq", "Ireland", "Israel", "Italy", "Jamaica", "Japan", "Jordan", "Kazakhstan", "Kenya", "Kiribati", "Kuwait", "Kyrgyzstan", "Latvia", "Lebanon", "Lesotho", "Liberia", "Liechtenstein", "Lithuania", "Luxembourg", "Macau", "Madagascar", "Malawi", "Malaysia", "Maldives", "Mali", "Malta", "Marshall", "Martinique", "Mauritania", "Mauritius", "Mayotte", "Mexico", "Micronesia", "Moldova", "Monaco", "Mongolia", "Montserrat", "Morocco", "Mozambique", "Myanmar", "Namibia", "Nauru", "Nepal", "Netherlands", "Netherlands", "Caledonia", "Zealand", "Nicaragua", "Niger", "Nigeria", "Niue", "Norfolk", "Mariana", "Norway", "Oman", "Pakistan", "Palau", "Panama", "Papua", "Paraguay", "Peru", "Philippines", "Pitcairn", "Poland", "Portugal", "Rico", "Qatar", "Reunion", "Romania", "Russian", "Rwanda", "Lucia", "Samoa", "Marino", "Arabia", "Senegal", "Seychelles", "Leone", "Singapore", "Slovakia", "Slovenia", "Solomon", "Somalia", "Africa", "Georgia", "Spain", "Sudan", "Suriname",  "Swaziland", "Sweden", "Switzerland", "Syrian Arab Republic", "Taiwan", "Tajikistan", "Thailand", "Togo", "Tokelau", "Tonga", "Tobago", "Tunisia", "Turkey", "Turkmenistan", "Tuvalu", "Uganda", "Ukraine", "Kingdom", "States", "Uruguay", "Uzbekistan", "Vanuatu", "Venezuela", "Vietnam", "Virgin", "Wallis", "Sahara", "Yemen", "Yugoslavia", "Zambia", "Zimbabwe")

val stopWords = List( "RT"," ","&","-","a", "about", "above", "after", "all", "am", "an", "and", "any", "are", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "could", "did", "do", "does", "doing", "down", "during", "each", "few", "for", "from", "further", "had", "has", "have", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "I", "I'd", "I'll", "I'm", "I've", "if", "in", "into", "is", "it", "it's", "its", "itself", "let's", "me", "more", "most", "my", "myself", "nor", "of", "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "she", "she'd", "she'll", "she's", "should", "so", "some", "such", "than", "that", "that's","The", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've","This", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "we", "we'd", "we'll", "we're", "we've", "were", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with", "would", "You","you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves" )

//Count words
val countWords = TweetAnalytics.countWords(longTweets)

//Count words from a user:
val countWordsUser = TweetAnalytics.countWords(tweetsFromUser)

//List of words most mentioned:
countWords.sortBy(-_._2).take(100).filter(t => !stopWords.contains(t._1)).foreach(t => println(t._1 + "\t "+ t._2))


//List of world most mentioned from a user:
countWordsUser.sortBy(-_._2).take(100).filter(t => !stopWords.contains(t._1)).foreach(t => println(t._1 + "\t "+ t._2))

//List of countries most mentioned:
countWords.sortBy(-_._2).take(105280).filter(t => countries.contains(t._1)).foreach(t => println(t._1 + "\t"+ t._2))

//List of cities most mentioned:
countWords.sortBy(-_._2).take(105280).filter(t => cities.contains(t._1)).foreach(t => println(t._1 + "\t"+ t._2))

