/*
What the code can do?

1> Doing time series analysis for SMW of each state from 1968 - 2020
2> Find mean SMW for all States from 1968 to 2020
3> Find state having max SMW for all years
4> Compare SMW and FMW year by Year for a given state to check states condition in that particular year
5> Plot Federal Minimum wage year By Year
6> Compute Effective Minimum wage and inflation based EMW by finding maximum of SMW and FMW for all states for all years
7> Computing inflation % change between a year and 2020 by dividing EMW2020 with EMW
8> Find states which don't meet change in EMW to change in Average CPI and notify if inflation need is not being met
9> Compare DOL Low and DOL high and find if a given state had two or more different min wages
10> Compare DOL Low and DOL Low 2020
11> Compare DOL High and DOL High 2020
*/

//------------------------------------------------------------------------------------------------------------------------------------------------

//Prerequisites(0)

spark-shell

var data = sc.textFile("minWageDataSample.csv")
//data: org.apache.spark.rdd.RDD[String] = minWageDataSample.csv MapPartitionsRDD[3] at textFile at <console>:24

val min_wage_rdd = data.map{l =>
     val s = l.split(',')
     val (year,state,smwy,smwe,fmwy,fmwe,avg,avgf,yl,el,yh,eh) = (s(0), s(1),  s(2),  s(3),  s(4),  s(5),  s(6),  s(7),  s(8),  s(9),  s(10),  s(11))
     (year,state,smwy,smwe,fmwy,fmwe,avg,avgf,yl,el,yh,eh)}
//min_wage_rdd: org.apache.spark.rdd.RDD[(String, String, String, String, String, String, String, String, String, String, String, String)] = MapPartitionsRDD[4] at map at <console>:25

min_wage_rdd.toDF.show(false)
/*
+----+--------------------+-------+-------+-------+-------+-------+-----------+------------+------------+-------------+-------------+
|_1  |_2                  |_3     |_4     |_5     |_6     |_7     |_8         |_9          |_10         |_11          |_12          |
+----+--------------------+-------+-------+-------+-------+-------+-----------+------------+------------+-------------+-------------+
|Year|State               |SMWYear|SMW2020|FMWYear|FMW2020|Avg CPI|Avg CPI2020|DOL Low Year|DOL Low 2020|DOL High Year|DOL High 2020|
|1968|Alabama             |0      |0      |1.15   |8.55   |34.8   |258.66     |0           |0           |0            |0            |
|1968|Alaska              |2.1    |15.61  |1.15   |8.55   |34.8   |258.66     |2.1         |15.61       |2.1          |15.61        |
|1968|Arizona             |0.468  |3.48   |1.15   |8.55   |34.8   |258.66     |0.468       |3.48        |0.66         |4.91         |
|1968|Arkansas            |0.15625|1.16   |1.15   |8.55   |34.8   |258.66     |0.15625     |1.16        |0.15625      |1.16         |
|1968|California          |1.65   |12.26  |1.15   |8.55   |34.8   |258.66     |1.65        |12.26       |1.65         |12.26        |
|1968|Colorado            |1      |7.43   |1.15   |8.55   |34.8   |258.66     |1           |7.43        |1.25         |9.29         |
|1968|Connecticut         |1.4    |10.41  |1.15   |8.55   |34.8   |258.66     |1.4         |10.41       |1.4          |10.41        |
|1968|Delaware            |1.25   |9.29   |1.15   |8.55   |34.8   |258.66     |1.25        |9.29        |1.25         |9.29         |
|1968|District of Columbia|1.25   |9.29   |1.15   |8.55   |34.8   |258.66     |1.25        |9.29        |1.4          |10.41        |
|1968|Florida             |0      |0      |1.15   |8.55   |34.8   |258.66     |0           |0           |0            |0            |
|1968|Georgia             |0      |0      |1.15   |8.55   |34.8   |258.66     |0           |0           |0            |0            |
|1968|Guam                |1.25   |9.29   |1.15   |8.55   |34.8   |258.66     |1.25        |9.29        |1.25         |9.29         |
|1968|Hawaii              |1.25   |9.29   |1.15   |8.55   |34.8   |258.66     |1.25        |9.29        |1.25         |9.29         |
|1968|Idaho               |1.15   |8.55   |1.15   |8.55   |34.8   |258.66     |1.15        |8.55        |1.15         |8.55         |
|1968|Illinois            |0      |0      |1.15   |8.55   |34.8   |258.66     |0           |0           |0            |0            |
|1968|Indiana             |1.15   |8.55   |1.15   |8.55   |34.8   |258.66     |1.15        |8.55        |1.15         |8.55         |
|1968|Iowa                |0      |0      |1.15   |8.55   |34.8   |258.66     |0           |0           |0            |0            |
|1968|Kansas              |0      |0      |1.15   |8.55   |34.8   |258.66     |0           |0           |0            |0            |
|1968|Kentucky            |0.65   |4.83   |1.15   |8.55   |34.8   |258.66     |0.65        |4.83        |0.75         |5.57         |
+----+--------------------+-------+-------+-------+-------+-------+-----------+------------+------------+-------------+-------------+
only showing top 20 rows
*/

val minWage = min_wage_rdd.filter(_._1 != "Year")
//minWage: org.apache.spark.rdd.RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String)] = MapPartitionsRDD[8] at filter at <console>:25

minWage.toDF("Year","State","SMW","SMW 2k20","FMW","FMW2k20","CPI","CPI 2k20","DOL low","DOL low 2k20","DOL high","DOL high 2k20").show(false)
/*
+----+--------------------+-------+--------+----+-------+----+--------+-------+------------+--------+-------------+
|Year|State               |SMW    |SMW 2k20|FMW |FMW2k20|CPI |CPI 2k20|DOL low|DOL low 2k20|DOL high|DOL high 2k20|
+----+--------------------+-------+--------+----+-------+----+--------+-------+------------+--------+-------------+
|1968|Alabama             |0      |0       |1.15|8.55   |34.8|258.66  |0      |0           |0       |0            |
|1968|Alaska              |2.1    |15.61   |1.15|8.55   |34.8|258.66  |2.1    |15.61       |2.1     |15.61        |
|1968|Arizona             |0.468  |3.48    |1.15|8.55   |34.8|258.66  |0.468  |3.48        |0.66    |4.91         |
|1968|Arkansas            |0.15625|1.16    |1.15|8.55   |34.8|258.66  |0.15625|1.16        |0.15625 |1.16         |
|1968|California          |1.65   |12.26   |1.15|8.55   |34.8|258.66  |1.65   |12.26       |1.65    |12.26        |
|1968|Colorado            |1      |7.43    |1.15|8.55   |34.8|258.66  |1      |7.43        |1.25    |9.29         |
|1968|Connecticut         |1.4    |10.41   |1.15|8.55   |34.8|258.66  |1.4    |10.41       |1.4     |10.41        |
|1968|Delaware            |1.25   |9.29    |1.15|8.55   |34.8|258.66  |1.25   |9.29        |1.25    |9.29         |
|1968|District of Columbia|1.25   |9.29    |1.15|8.55   |34.8|258.66  |1.25   |9.29        |1.4     |10.41        |
|1968|Florida             |0      |0       |1.15|8.55   |34.8|258.66  |0      |0           |0       |0            |
|1968|Georgia             |0      |0       |1.15|8.55   |34.8|258.66  |0      |0           |0       |0            |
|1968|Guam                |1.25   |9.29    |1.15|8.55   |34.8|258.66  |1.25   |9.29        |1.25    |9.29         |
|1968|Hawaii              |1.25   |9.29    |1.15|8.55   |34.8|258.66  |1.25   |9.29        |1.25    |9.29         |
|1968|Idaho               |1.15   |8.55    |1.15|8.55   |34.8|258.66  |1.15   |8.55        |1.15    |8.55         |
|1968|Illinois            |0      |0       |1.15|8.55   |34.8|258.66  |0      |0           |0       |0            |
|1968|Indiana             |1.15   |8.55    |1.15|8.55   |34.8|258.66  |1.15   |8.55        |1.15    |8.55         |
|1968|Iowa                |0      |0       |1.15|8.55   |34.8|258.66  |0      |0           |0       |0            |
|1968|Kansas              |0      |0       |1.15|8.55   |34.8|258.66  |0      |0           |0       |0            |
|1968|Kentucky            |0.65   |4.83    |1.15|8.55   |34.8|258.66  |0.65   |4.83        |0.75    |5.57         |
|1968|Louisiana           |0      |0       |1.15|8.55   |34.8|258.66  |0      |0           |0       |0            |
+----+--------------------+-------+--------+----+-------+----+--------+-------+------------+--------+-------------+
only showing top 20 rows
*/

//------------------------------------------------------------------------------------------------------------------------------------------------

//Question1: Doing time series analysis for SMW of each state from 1968 - 2020

val smwTime = minWage.map(l => (l._2,(l._1,l._3)))
//smwTime: org.apache.spark.rdd.RDD[(String, (String, String))] = MapPartitionsRDD[18] at map at <console>:25

val alaskaData = smwTime.filter(_._1 == "Alaska")
//alaskaData: org.apache.spark.rdd.RDD[(String, (String, String))] = MapPartitionsRDD[19] at filter at <console>:25

alaskaData.toDF("State","Year and Wage").show(false)
/*
+------+-------------+
|State |Year and Wage|
+------+-------------+
|Alaska|{1968, 2.1}  |
|Alaska|{1969, 2.1}  |
|Alaska|{1970, 2.1}  |
|Alaska|{1971, 2.1}  |
|Alaska|{1972, 2.1}  |
|Alaska|{1973, 2.1}  |
|Alaska|{1974, 2.1}  |
|Alaska|{1975, 2.1}  | 
|Alaska|{1976, 2.8}  |
|Alaska|{1977, 2.8}  |
|Alaska|{1978, 2.8}  |
|Alaska|{1979, 3.4}  |
|Alaska|{1980, 3.6}  |
|Alaska|{1981, 3.85} |
|Alaska|{1982, 3.85} |
|Alaska|{1983, 3.85} |
|Alaska|{1984, 3.85} |
|Alaska|{1985, 3.85} |
|Alaska|{1986, 3.85} |
|Alaska|{1987, 3.85} |
+------+-------------+
only showing top 20 rows
*/

//------------------------------------------------------------------------------------------------------------------------------------------------

//Question 2: Find mean SMW for all States from 1968 to 2020

val smw_inc = minWage.map{ l => (l._2, (l._1, l._3)) }
//smw_inc: org.apache.spark.rdd.RDD[(String, (String, String))] = ShuffledRDD[51] at sortByKey at <console>:25

val smw_meanbystate = smw_inc.map{ case (st, (yr, smw)) => (st, smw.toDouble/53.0) }.reduceByKey(_+_).sortBy(_._2, false)
//smw_meanbystate: org.apache.spark.rdd.RDD[(String, Double)] = MapPartitionsRDD[107] at sortBy at <console>:25

smw_meanbystate.toDF("State", "Mean of SMW by State").show(false)
/*
+--------------------+--------------------+
|STATE               |MEAN OF SMW BY STATE|
+--------------------+--------------------+
|Alaska              |5.2749056603773585  |
|District of Columbia|5.2501886792452845  |
|Oregon              |5.244339622641508   |
|Massachusetts       |5.24245283018868    |
|Connecticut         |5.194905660377356   |
|Washington          |5.1864150943396226  |
|California          |5.175471698113207   |
|Vermont             |5.140377358490567   |
|Rhode Island        |4.9292452830188696  |
|Maine               |4.848113207547169   |
|Hawaii              |4.843396226415093   |
|New York            |4.820754716981131   |
|New Jersey          |4.748113207547169   |
|Maryland            |4.645283018867923   |
|Michigan            |4.540566037735848   |
|Guam                |4.4990566037735835  |
|Delaware            |4.489622641509434   |
|Illinois            |4.487735849056602   |
|Pennsylvania        |4.412264150943394   |
|New Hampshire       |4.4084905660377345  |
+--------------------+--------------------+
only showing top 20 rows
*/

def dumpToCSV[S](a: Array[S], fileName: String, title: String = "") {
	val file = new java.io.PrintStream(fileName)
	file.println(title)
	a.foreach{ l =>
 	val str = l.toString.replaceAll("\\(","").replaceAll("\\)","") 
	file.println(str)
	}
	file.close 
}

//dumpToCSV: [S](a: Array[S], fileName: String, title: String)Unit

val smw_meanbystate_fin = smw_meanbystate.collect
//smw_meanbystate_fin: Array[(String, Double)] = Array((Alaska,5.2749056603773585), (District of Columbia,5.2501886792452845), (Oregon,5.244339622641508), (Massachusetts,5.24245283018868), (Connecticut,5.194905660377356), (Washington,5.1864150943396226), (California,5.175471698113207), (Vermont,5.140377358490567), (Rhode Island,4.9292452830188696), (Maine,4.848113207547169), (Hawaii,4.843396226415093), (New York,4.820754716981131), (New Jersey,4.748113207547169), (Maryland,4.645283018867923), (Michigan,4.540566037735848), (Guam,4.4990566037735835), (Delaware,4.489622641509434), (Illinois,4.487735849056602), (Pennsylvania,4.412264150943394), (New Hampshire,4.4084905660377345), (Wisconsin,4.35566037735849), (West Virginia,4.333018867924529), (Nevada,4.2854716981132...

dumpToCSV(smw_meanbystate_fin, "MeanSMWperYear.csv", "STATE, MEAN SMW BY STATE")


//------------------------------------------------------------------------------------------------------------------------------------------------

//Question 3: Find state having max SMW for all years

for(i<- 1968 until 2020){
	val maxInYear = minWage.filter(_._1 == i.toString).map(l=>((l._1.toInt,l._2),l._3.toDouble)).map{case ((yr,st),sm) => sm}.reduce(_ max _)
	val maxStateinYear = minWage.filter(_._1 == i.toString).map(l=>((l._1.toInt,l._2),l._3.toDouble)).filter(_._2 == maxInYear).collect().toList
	println(maxStateinYear)
}
/*
List(((1968,Alaska),2.1))
List(((1969,Alaska),2.1))
List(((1970,Alaska),2.1))
List(((1971,Alaska),2.1))
List(((1972,Alaska),2.1))
List(((1973,Alaska),2.1))
List(((1974,Alaska),2.1))
List(((1975,Alaska),2.1))
List(((1976,Alaska),2.8))
List(((1977,Alaska),2.8))
List(((1978,Alaska),2.8))
List(((1979,Alaska),3.4))
List(((1980,Alaska),3.6))
List(((1981,Alaska),3.85))
List(((1982,Alaska),3.85))
List(((1983,Alaska),3.85))
List(((1984,Alaska),3.85))
List(((1985,Alaska),3.85))
List(((1986,Alaska),3.85))
List(((1987,Alaska),3.85))
List(((1988,Alaska),3.85))
List(((1989,Alaska),3.85))
List(((1990,Alaska),3.85))
List(((1991,Oregon),4.75))
List(((1992,Alaska),4.75))
List(((1993,Alaska),4.75))
List(((1994,Hawaii),5.25))
List(((1995,Hawaii),5.25))
List(((1996,District of Columbia),5.25))
List(((1997,District of Columbia),5.75))
List(((1998,District of Columbia),6.15))
List(((1999,District of Columbia),6.15))
List(((2000,Oregon),6.5), ((2000,Washington),6.5))
List(((2001,Massachusetts),6.75))
List(((2002,Washington),6.9))
List(((2003,Alaska),7.15))
List(((2004,Washington),7.16))
List(((2005,Washington),7.35))
List(((2006,Washington),7.63))
List(((2007,Washington),7.93))
List(((2008,Washington),8.07))
List(((2009,Washington),8.55))
List(((2010,Washington),8.55))
List(((2011,Washington),8.67))
List(((2012,Washington),9.04))
List(((2013,Washington),9.19))
List(((2014,District of Columbia),9.5))
List(((2015,District of Columbia),10.5))
List(((2016,District of Columbia),11.5))
List(((2017,District of Columbia),11.5))
List(((2018,District of Columbia),13.25))
List(((2019,District of Columbia),14.0))
*/

// -----------------------------------------------------------------------------------------------------------------------------------

//Question 4: Compare SMW and FMW year by year for a given state

var goodStates = minWage.map(l => (l._1,l._2,l._3,l._5,l._3.toFloat - l._5.toFloat)).filter(_._5 > 0.0).map{case (yr,st,sw,fw,dif) => (yr,st,sw,fw)}  
//goodStates: org.apache.spark.rdd.RDD[(String, String, String, String)] = MapPartitionsRDD[39] at map at <console>:25                                         
                                                                                                                                                             
goodStates.toDF("Year","State","SMW","FMW").show(false)
/*
+----+--------------------+----+----+
|Year|State               |SMW |FMW |
+----+--------------------+----+----+
|1968|Alaska              |2.1 |1.15|
|1968|California          |1.65|1.15|
|1968|Connecticut         |1.4 |1.15|
|1968|Delaware            |1.25|1.15|
|1968|District of Columbia|1.25|1.15|
|1968|Guam                |1.25|1.15|
|1968|Hawaii              |1.25|1.15|
|1968|Maine               |1.4 |1.15|
|1968|Massachusetts       |1.6 |1.15|
|1968|Michigan            |1.25|1.15|
|1968|Nevada              |1.25|1.15|
|1968|New Hampshire       |1.4 |1.15|
|1968|New Jersey          |1.4 |1.15|
|1968|New York            |1.6 |1.15|
|1968|Oregon              |1.25|1.15|
|1968|Rhode Island        |1.4 |1.15|
|1968|Vermont             |1.4 |1.15|
|1968|Washington          |1.6 |1.15|
|1968|Wisconsin           |1.25|1.15|
|1968|Wyoming             |1.2 |1.15|
+----+--------------------+----+----+
only showing top 20 rows                                                                                                                                    
*/                                                                                                                                                           
                                                                                                                                                             
var badStates = minWage.map(l => (l._1,l._2,l._3,l._5,l._3.toFloat - l._5.toFloat)).filter(_._5 < 0.0).map{case (yr,st,sw,fw,dif) => (yr,st,sw,fw)}   
//badStates: org.apache.spark.rdd.RDD[(String, String, String, String)] = MapPartitionsRDD[45] at map at <console>:25                                          
                                                                                                                                                             
badStates.toDF("Year","State","SMW","FMW").show(false)
/*
+----+--------------+-------+----+
|Year|State         |SMW    |FMW |
+----+--------------+-------+----+
|1968|Alabama       |0      |1.15|
|1968|Arizona       |0.468  |1.15|
|1968|Arkansas      |0.15625|1.15|
|1968|Colorado      |1      |1.15|
|1968|Florida       |0      |1.15|
|1968|Georgia       |0      |1.15|
|1968|Illinois      |0      |1.15|
|1968|Iowa          |0      |1.15|
|1968|Kansas        |0      |1.15|
|1968|Kentucky      |0.65   |1.15|
|1968|Louisiana     |0      |1.15|
|1968|Maryland      |1      |1.15|
|1968|Minnesota     |0.7    |1.15|
|1968|Mississippi   |0      |1.15|
|1968|Missouri      |0      |1.15|
|1968|Montana       |0      |1.15|
|1968|Nebraska      |1      |1.15|
|1968|North Carolina|1      |1.15|
|1968|North Dakota  |1      |1.15|
|1968|Ohio          |0.75   |1.15|
+----+--------------+-------+----+
only showing top 20 rows
*/

//------------------------------------------------------------------------------------------------------------------------------------------------
//Question 5: Compare SMW and FMW year by Year for a given state to check states condition in that particular year

val fmwAllYears = minWage.map(l=>(l._1,l._5.toDouble)).reduceByKey(_ max _).sortByKey()
//fmwAllYears: org.apache.spark.rdd.RDD[(String, Double)] = ShuffledRDD[21] at sortByKey at <console>:25

fmwAllYears.toDF("Year","FMW").show(false)
/*
+----+----+
|Year|FMW |
+----+----+
|1968|1.15|
|1969|1.15|
|1970|1.3 |
|1971|1.3 |
|1972|1.6 |
|1973|1.6 |
|1974|1.6 |
|1975|1.6 |
|1976|2.2 |
|1977|2.2 |
|1978|2.2 |
|1979|2.9 |
|1980|3.1 |
|1981|3.35|
|1982|3.35|
|1983|3.35|
|1984|3.35|
|1985|3.35|
|1986|3.35|
|1987|3.35|
+----+----+
only showing top 20 rows
*/

//------------------------------------------------------------------------------------------------------------------------------------------------
//Question 6: Compute Effective Minimum wage and inflation based EMW by finding maximun of SMW and FMW for all stattes for all years

val emw = minWage.map{ l => ((l._1,l._2),if (l._3.toDouble>l._5.toDouble) l._3.toDouble else l._5.toDouble) }
//emw: org.apache.spark.rdd.RDD[((String, String), Double)] = MapPartitionsRDD[382] at map at <console>:25

emw.toDF("Year and State","EMW").show(false)
/*
+----------------------------+----+
|Year and State              |EMW |
+----------------------------+----+
|{1968, Alabama}             |1.15|
|{1968, Alaska}              |2.1 |
|{1968, Arizona}             |1.15|
|{1968, Arkansas}            |1.15|
|{1968, California}          |1.65|
|{1968, Colorado}            |1.15|
|{1968, Connecticut}         |1.4 |
|{1968, Delaware}            |1.25|
|{1968, District of Columbia}|1.25|
|{1968, Florida}             |1.15|
|{1968, Georgia}             |1.15|
|{1968, Guam}                |1.25|
|{1968, Hawaii}              |1.25|
|{1968, Idaho}               |1.15|
|{1968, Illinois}            |1.15|
|{1968, Indiana}             |1.15|
|{1968, Iowa}                |1.15|
|{1968, Kansas}              |1.15|
|{1968, Kentucky}            |1.15|
|{1968, Louisiana}           |1.15|
+----------------------------+----+
only showing top 20 rows
*/

val emw_2k20 = minWage.map{ l => ((l._1,l._2),if (l._4.toDouble>l._6.toDouble) l._4.toDouble else l._6.toDouble) }
//emw_2k20: org.apache.spark.rdd.RDD[((String, String), Double)] = MapPartitionsRDD[386] at map at <console>:25

emw_2k20.toDF("Year and State","EMW Inflation").show(false)
/*
+----------------------------+-------------+
|Year and State              |EMW Inflation|
+----------------------------+-------------+
|{1968, Alabama}             |8.55         |
|{1968, Alaska}              |15.61        |
|{1968, Arizona}             |8.55         |
|{1968, Arkansas}            |8.55         |
|{1968, California}          |12.26        |
|{1968, Colorado}            |8.55         |
|{1968, Connecticut}         |10.41        |
|{1968, Delaware}            |9.29         |
|{1968, District of Columbia}|9.29         |
|{1968, Florida}             |8.55         |
|{1968, Georgia}             |8.55         |
|{1968, Guam}                |9.29         |
|{1968, Hawaii}              |9.29         |
|{1968, Idaho}               |8.55         |
|{1968, Illinois}            |8.55         |
|{1968, Indiana}             |8.55         |
|{1968, Iowa}                |8.55         |
|{1968, Kansas}              |8.55         |
|{1968, Kentucky}            |8.55         |
|{1968, Louisiana}           |8.55         |
+----------------------------+-------------+
only showing top 20 rows
*/

//------------------------------------------------------------------------------------------------------------------------------------------------

//Question 7: Computing inflation % change between a year and 2020 by dividing EMW2020 with EMW

val inflation = emw_2k20.join(emw).map{ case((yr,st),(emw2k20,emw))=>((yr,st),emw2k20/emw*100)}.sortByKey()
//inflation: org.apache.spark.rdd.RDD[((String, String), Double)] = ShuffledRDD[421] at sortByKey at <console>:27

inflation.toDF("Year and State","Inflation").show(false)
/*
+----------------------------+-----------------+
|Year and State              |Inflation        |
+----------------------------+-----------------+
|{1968, Alabama}             |743.4782608695654|
|{1968, Alaska}              |743.3333333333333|
|{1968, Arizona}             |743.4782608695654|
|{1968, Arkansas}            |743.4782608695654|
|{1968, California}          |743.0303030303031|
|{1968, Colorado}            |743.4782608695654|
|{1968, Connecticut}         |743.5714285714286|
|{1968, Delaware}            |743.1999999999999|
|{1968, District of Columbia}|743.1999999999999|
|{1968, Florida}             |743.4782608695654|
|{1968, Georgia}             |743.4782608695654|
|{1968, Guam}                |743.1999999999999|
|{1968, Hawaii}              |743.1999999999999|
|{1968, Idaho}               |743.4782608695654|
|{1968, Illinois}            |743.4782608695654|
|{1968, Indiana}             |743.4782608695654|
|{1968, Iowa}                |743.4782608695654|
|{1968, Kansas}              |743.4782608695654|
|{1968, Kentucky}            |743.4782608695654|
|{1968, Louisiana}           |743.4782608695654|
+----------------------------+-----------------+
only showing top 20 rows
*/

//------------------------------------------------------------------------------------------------------------------------------------------------

//Question 8: Find states which don't meet change in EMW to change in Average CPI and notify if inflation need is not being met

val cpiGrowth = minWage.map{ l => ((l._1,l._2),(l._8.toDouble/l._7.toDouble)*100)}
//cpiGrowth: org.apache.spark.rdd.RDD[((String, String), Double)] = MapPartitionsRDD[447] at map at <console>:25

cpiGrowth.toDF("Year and State","CPI Growth").show(false)
/*
+----------------------------+-----------------+
|Year and State              |CPI Growth       |
+----------------------------+-----------------+
|{1968, Alabama}             |743.2758620689657|
|{1968, Alaska}              |743.2758620689657|
|{1968, Arizona}             |743.2758620689657|
|{1968, Arkansas}            |743.2758620689657|
|{1968, California}          |743.2758620689657|
|{1968, Colorado}            |743.2758620689657|
|{1968, Connecticut}         |743.2758620689657|
|{1968, Delaware}            |743.2758620689657|
|{1968, District of Columbia}|743.2758620689657|
|{1968, Florida}             |743.2758620689657|
|{1968, Georgia}             |743.2758620689657|
|{1968, Guam}                |743.2758620689657|
|{1968, Hawaii}              |743.2758620689657|
|{1968, Idaho}               |743.2758620689657|
|{1968, Illinois}            |743.2758620689657|
|{1968, Indiana}             |743.2758620689657|
|{1968, Iowa}                |743.2758620689657|
|{1968, Kansas}              |743.2758620689657|
|{1968, Kentucky}            |743.2758620689657|
|{1968, Louisiana}           |743.2758620689657|
+----------------------------+-----------------+
only showing top 20 rows
*/

val passiveStates = cpiGrowth.join(inflation)
//passiveStates: org.apache.spark.rdd.RDD[((String, String), (Double, Double))] = MapPartitionsRDD[453] at join at <console>:27

passiveStates.toDF("Year and State","Inflation and CPI").show(false)
/*
+----------------------------+----------------------------------------+
|Year and State              |Inflation and CPI                       |
+----------------------------+----------------------------------------+
|{1979, Colorado}            |{356.28099173553727, 356.2068965517242} |
|{1980, District of Columbia}|{313.90776699029124, 313.8709677419355} |
|{1968, District of Columbia}|{743.2758620689657, 743.1999999999999}  |
|{1988, South Carolina}      |{218.64750633981407, 218.50746268656715}|
|{1971, Washington}          |{638.6666666666667, 638.75}             |
|{1979, Kansas}              |{356.28099173553727, 356.2068965517242} |
|{1980, Georgia}             |{313.90776699029124, 313.8709677419355} |
|{1973, Florida}             |{582.5675675675677, 582.5}              |
|{1968, Mississippi}         |{743.2758620689657, 743.4782608695654}  |
|{1970, North Dakota}        |{666.6494845360826, 666.9230769230769}  |
|{1971, Oregon}              |{638.6666666666667, 638.4615384615385}  |
|{1982, New Mexico}          |{268.0414507772021, 268.05970149253733} |
|{1980, Indiana}             |{313.90776699029124, 313.8709677419355} |
|{1991, South Carolina}      |{189.91189427312779, 190.0}             |
|{1980, South Dakota}        |{313.90776699029124, 313.8709677419355} |
|{1982, Montana}             |{268.0414507772021, 268.05970149253733} |
|{1992, Rhode Island}        |{184.36208125445475, 184.2696629213483} |
|{1970, Guam}                |{666.6494845360826, 666.8749999999999}  |
|{1988, Iowa}                |{218.64750633981407, 218.50746268656715}|
|{1990, Kentucky}            |{197.90359602142314, 197.910447761194}  |
+----------------------------+----------------------------------------+
only showing top 20 rows
*/

val passive = cpiGrowth.join(inflation).map{case ((yr,st),(in,cp))=>(yr,st,in-cp)}.filter(_._3 >= 0).map{case(a,b,c)=>(a,b)}
//passive: org.apache.spark.rdd.RDD[(String, String)] = MapPartitionsRDD[462] at map at <console>:27

passive.toDF("Year","Passive Paying State").show(false)
/*
+----+--------------------+
|Year|Passive Paying State|
+----+--------------------+
|1979|Colorado            |
|1980|District of Columbia|
|1968|District of Columbia|
|1988|South Carolina      |
|1979|Kansas              |
|1980|Georgia             |
|1973|Florida             |
|1971|Oregon              |
|1980|Indiana             |
|1980|South Dakota        |
|1992|Rhode Island        |
|1988|Iowa                |
|1972|New Mexico          |
|1968|Hawaii              |
|1988|Oklahoma            |
|1979|California          |
|1975|South Dakota        |
|1972|Wyoming             |
|1973|District of Columbia|
|1974|Missouri            |
+----+--------------------+
only showing top 20 rows
*/

//------------------------------------------------------------------------------------------------------------------------------------------------

//Question 9: Compare DOL Low and DOL high and find if a given state had two or more different min wages

val dolCompare = minWage.map(l => ((l._1,l._2),(l._10,l._12),l._10.toDouble - l._12.toDouble)).filter(_._3 != 0).map{case (a,b,c)=>(a,b)}
//dolCompare: org.apache.spark.rdd.RDD[((String, String), (String, String))] = MapPartitionsRDD[28] at map at <console>:25

dolCompare.toDF("Year and State","DOL Wages").show(false) 
/*
+----------------------------+-------------+
|Year and State              |DOL Wages    |
+----------------------------+-------------+
|{1968, Arizona}             |{0.468, 0.66}|
|{1968, Colorado}            |{1, 1.25}    |
|{1968, District of Columbia}|{1.25, 1.4}  |
|{1968, Kentucky}            |{0.65, 0.75} |
|{1968, Maryland}            |{1, 1.15}    |
|{1968, Minnesota}           |{0.7, 1.15}  |
|{1968, New Mexico}          |{1.15, 1.4}  |
|{1968, North Dakota}        |{1, 1.25}    |
|{1968, Ohio}                |{0.75, 1.25} |
|{1968, Puerto Rico}         |{0.43, 1.6}  |
|{1968, South Dakota}        |{0.425, 0.5} |
|{1968, Utah}                |{1, 1.15}    |
|{1969, Arizona}             |{0.468, 0.66}|
|{1969, Colorado}            |{1, 1.25}    |
|{1969, District of Columbia}|{1.25, 1.4}  |
|{1969, Kentucky}            |{0.65, 0.75} |
|{1969, Maryland}            |{1, 1.15}    |
|{1969, Minnesota}           |{0.7, 1.15}  |
|{1969, New Mexico}          |{1.15, 1.4}  |
|{1969, North Dakota}        |{1, 1.25}    |
+----------------------------+-------------+
only showing top 20 rows
*/

val compDOLlow = minWage.map(l => ((l._1,l._2),(l._9,l._10)))
//compDOLlow: org.apache.spark.rdd.RDD[((String, String), (String, String))] = MapPartitionsRDD[503] at map at <console>:25

//------------------------------------------------------------------------------------------------------------------------------------------------

// Question 10: Compare DOL Low and DOL Low 2020

compDOLlow.toDF("Year and State","DOL low and 2k20").show(false)
/*
+----------------------------+----------------+
|Year and State              |DOL low and 2k20|
+----------------------------+----------------+
|{1968, Alabama}             |{0, 0}          |
|{1968, Alaska}              |{2.1, 15.61}    |
|{1968, Arizona}             |{0.468, 3.48}   |
|{1968, Arkansas}            |{0.15625, 1.16} |
|{1968, California}          |{1.65, 12.26}   |
|{1968, Colorado}            |{1, 7.43}       |
|{1968, Connecticut}         |{1.4, 10.41}    |
|{1968, Delaware}            |{1.25, 9.29}    |
|{1968, District of Columbia}|{1.25, 9.29}    |
|{1968, Florida}             |{0, 0}          |
|{1968, Georgia}             |{0, 0}          |
|{1968, Guam}                |{1.25, 9.29}    |
|{1968, Hawaii}              |{1.25, 9.29}    |
|{1968, Idaho}               |{1.15, 8.55}    |
|{1968, Illinois}            |{0, 0}          |
|{1968, Indiana}             |{1.15, 8.55}    |
|{1968, Iowa}                |{0, 0}          |
|{1968, Kansas}              |{0, 0}          |
|{1968, Kentucky}            |{0.65, 4.83}    |
|{1968, Louisiana}           |{0, 0}          |
+----------------------------+----------------+
only showing top 20 rows
*/

//------------------------------------------------------------------------------------------------------------------------------------------------

// Question 11: Compare DOL High and DOL High 2020

val compDOLhigh = minWage.map(l => ((l._1,l._2),(l._11,l._12)))
//compDOLhigh: org.apache.spark.rdd.RDD[((String, String), (String, String))] = MapPartitionsRDD[507] at map at <console>:25

compDOLhigh.toDF("Year and State","DOL high and 2k20").show(false)
/*
+----------------------------+-----------------+
|Year and State              |DOL high and 2k20|
+----------------------------+-----------------+
|{1968, Alabama}             |{0, 0}           |
|{1968, Alaska}              |{2.1, 15.61}     |
|{1968, Arizona}             |{0.66, 4.91}     |
|{1968, Arkansas}            |{0.15625, 1.16}  |
|{1968, California}          |{1.65, 12.26}    |
|{1968, Colorado}            |{1.25, 9.29}     |
|{1968, Connecticut}         |{1.4, 10.41}     |
|{1968, Delaware}            |{1.25, 9.29}     |
|{1968, District of Columbia}|{1.4, 10.41}     |
|{1968, Florida}             |{0, 0}           |
|{1968, Georgia}             |{0, 0}           |
|{1968, Guam}                |{1.25, 9.29}     |
|{1968, Hawaii}              |{1.25, 9.29}     |
|{1968, Idaho}               |{1.15, 8.55}     |
|{1968, Illinois}            |{0, 0}           |
|{1968, Indiana}             |{1.15, 8.55}     |
|{1968, Iowa}                |{0, 0}           |
|{1968, Kansas}              |{0, 0}           |
|{1968, Kentucky}            |{0.75, 5.57}     |
|{1968, Louisiana}           |{0, 0}           |
+----------------------------+-----------------+
only showing top 20 rows
*/
