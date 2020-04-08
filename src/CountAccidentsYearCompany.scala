import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{monotonically_increasing_id, explode, first}
import scala.collection.mutable.HashMap 
import scala.io.Source

//HashMap to JSON
import java.nio.charset.StandardCharsets
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import java.io._




object CountAccidentsYearCompany extends App {
	var hashMap:HashMap[String,HashMap[String,Int]] = 
			HashMap(
			    ("2019",HashMap()),
					("2018",HashMap()),
					("2017",HashMap()),
					("2016",HashMap()),
					("2015",HashMap()),
					("2014",HashMap())
					)

			val yearList = Array("2019","2018","2017","2016","2015","2014")
      var year = "2019"
			val bufferedSource = scala.io.Source.fromFile("C://Users//Christian//Desktop//JoachimsPaper//SelfDrivingCars.csv")
			var lineIter = bufferedSource.getLines().drop(0) //discard header
			lineIter.next()
			for (line <- lineIter) {
				val cols = line.split(",")
						for (i <- 0 to cols.length-1){
						  year = yearList(i)
							val words = cols(i).split(" ").map(_.trim) 
							if(words!=null){
										val name = words(0).replace('"',' ').trim()
										var companyMap = hashMap(year)
										var count:Int = 1
										if(companyMap.contains(name)){
										  count = companyMap(name)
										  count += 1
										  println(count,name,year)
										}
										companyMap.put(name,count)
										hashMap.put(year,companyMap)
							}
							else println("words null" + cols)
						}
			}
			
			//println(hashMap(year))
			
			bufferedSource.close

			//Convert HashMap to JSON
			implicit val json4sFormats = DefaultFormats
			//val yourBytes: Array[Byte] = "somebytes".getBytes(StandardCharsets.UTF_8)
			//val list: List[String] = List("a", "aa")
			//val map = Map("query" -> yourBytes, "abc" -> list)
			val json = Serialization.writePretty(hashMap.toMap)
			//println(json)

			var listBuffer = new scala.collection.mutable.ListBuffer[String]()
			for ((k_year,v_map) <- hashMap){
			  for((k_company,v_count) <- v_map){
			    listBuffer += new String(k_year+","+k_company+","+v_count.toString()+"\n")
			  }
			}
			// println(listBuffer(1))

			  
			val file = new File("C://Users//Christian//Desktop//JoachimsPaper//AccidentCount.csv")
			var bw = new BufferedWriter(new FileWriter(file))
			bw.write(new String("year,company,accidents"+"\n"))
			listBuffer.foreach {
			   bw.write
			}
			bw.close()
			
			
//Write results into a Spark dataframe
//val DataFrame df = df.withColumn("id", (org.apache.spark.sql.functions.monotonically_increasing_id()))
// .select($"id", explode(hashMap))
//  .groupBy("id")
//  .pivot("key")
//  .agg(first("value"))

// dataFrame.write.format("com.databricks.spark.csv").save("myFile.csv")
  
//Count occurrences by company and year
}