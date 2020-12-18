/*** case classes ***/

//for dataSet_ scala file
case class Movies(imdb_title_id:String,title:String,original_title:String,year:Int,date_published:String,genre:String,duration:Int,country:String,language:String,director:String,writer:String,	production_company:String,actors:String,description:String,avg_vote:String,votes:String,budget:String,usa_gross_income:String,worlwide_gross_income:String,metascore:String,reviews_from_users:String,reviews_from_critics:String)

//for dataFramedataSet scala file for csv dataset
case class Movies1(title:String,original_title:String,year:String,date_published:String,genre:String,duration:String,country:String,language:String,director:String,writer:String,	production_company:String,actors:String,description:String,avg_vote:String,votes:String,budget:String,usa_gross_income:String,metascore:String,reviews_from_users:String,reviews_from_critics:String)

//for dataFramedataSet scala file for json dataset
case class Movie(imdb_title_id:String,title:String,original_title:String,year:String,date_published:String,genre:String,duration:String,country:String,language:String,director:String,writer:String,	production_company:String,actors:String,description:String,avg_vote:String,votes:String,budget:String,usa_gross_income:String,worlwide_gross_income:String,metascore:String,reviews_from_users:String,reviews_from_critics:String)

object queryCaseClasses {

}
