## CARLOS SÁNCHEZ VEGA

# DATAFRAME EXERCISES

We import libraries:


```python
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func
from pyspark.sql.functions import col, asc,desc
```

We create the spark configuration:


```python
spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()
```


```python
lines = spark.read.option("header", "true").option("inferSchema", "true").csv("fakefriends-header.csv")
```


```python
lines.show()
```

    +------+--------+---+-------+
    |userID|    name|age|friends|
    +------+--------+---+-------+
    |     0|    Will| 33|    385|
    |     1|Jean-Luc| 26|      2|
    |     2|    Hugh| 55|    221|
    |     3|  Deanna| 40|    465|
    |     4|   Quark| 68|     21|
    |     5|  Weyoun| 59|    318|
    |     6|  Gowron| 37|    220|
    |     7|    Will| 54|    307|
    |     8|  Jadzia| 38|    380|
    |     9|    Hugh| 27|    181|
    |    10|     Odo| 53|    191|
    |    11|     Ben| 57|    372|
    |    12|   Keiko| 54|    253|
    |    13|Jean-Luc| 56|    444|
    |    14|    Hugh| 43|     49|
    |    15|     Rom| 36|     49|
    |    16|  Weyoun| 22|    323|
    |    17|     Odo| 35|     13|
    |    18|Jean-Luc| 45|    455|
    |    19|  Geordi| 60|    246|
    +------+--------+---+-------+
    only showing top 20 rows
    


<ul>
<li>First column: identifier</li>
<li>Second column: name</li>
<li>Third column: age</li>
<li>Fourth column: number of friends</li>
</ul> 


```python
lines.printSchema()
```

    root
     |-- userID: integer (nullable = true)
     |-- name: string (nullable = true)
     |-- age: integer (nullable = true)
     |-- friends: integer (nullable = true)
    



```python
1. <strong>Count the number of people aged 21, at most</strong>
```


      File "<ipython-input-14-fa7555bb2397>", line 1
        1. <strong>Count the number of people aged 21, at most</strong>
                         ^
    SyntaxError: invalid syntax



2. <strong>Get the average number of friends broken down by age using dataframes</strong>

In this case, we should select the columns of age and friends

The are asking us to get the number of occurrences of a mark in the RDD, so we could use, for example:


```python
friendsAndAge=lines.select("age", "friends")
```


```python
friendsAndAge.show()
```

    +---+-------+
    |age|friends|
    +---+-------+
    | 33|    385|
    | 26|      2|
    | 55|    221|
    | 40|    465|
    | 68|     21|
    | 59|    318|
    | 37|    220|
    | 54|    307|
    | 38|    380|
    | 27|    181|
    | 53|    191|
    | 57|    372|
    | 54|    253|
    | 56|    444|
    | 43|     49|
    | 36|     49|
    | 22|    323|
    | 35|     13|
    | 45|    455|
    | 60|    246|
    +---+-------+
    only showing top 20 rows
    


We get the average number of friends per age


```python
friendsAndAge.groupBy("age").avg("friends").show()
```

    +---+------------------+
    |age|      avg(friends)|
    +---+------------------+
    | 31|            267.25|
    | 65|             298.2|
    | 53|222.85714285714286|
    | 34|             245.5|
    | 28|             209.1|
    | 26|242.05882352941177|
    | 27|           228.125|
    | 44| 282.1666666666667|
    | 22|206.42857142857142|
    | 47|233.22222222222223|
    | 52| 340.6363636363636|
    | 40| 250.8235294117647|
    | 20|             165.0|
    | 57| 258.8333333333333|
    | 54| 278.0769230769231|
    | 48|             281.4|
    | 19|213.27272727272728|
    | 64| 281.3333333333333|
    | 41|268.55555555555554|
    | 43|230.57142857142858|
    +---+------------------+
    only showing top 20 rows
    


We can see we can format the output as:
   * We can limit number of decimals shown
   * We can sort the result to find which is the age most friendly (considered as the one having the max number of friends)
   * We can change column name "avg(friends)" for a more readible name


```python
friendsAndAge.groupBy("age").agg(func.round(func.avg("friends"), 2)
  .alias("friends_avg")).sort("age").show()
```

    +---+-----------+
    |age|friends_avg|
    +---+-----------+
    | 18|     343.38|
    | 19|     213.27|
    | 20|      165.0|
    | 21|     350.88|
    | 22|     206.43|
    | 23|      246.3|
    | 24|      233.8|
    | 25|     197.45|
    | 26|     242.06|
    | 27|     228.13|
    | 28|      209.1|
    | 29|     215.92|
    | 30|     235.82|
    | 31|     267.25|
    | 32|     207.91|
    | 33|     325.33|
    | 34|      245.5|
    | 35|     211.63|
    | 36|      246.6|
    | 37|     249.33|
    +---+-----------+
    only showing top 20 rows
    



```python
spark.stop()
```

3. <strong>Get the the number of people who are teenagers (aged 13 - 19) and the number of people by age</strong>


```python
spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()
```


```python
lines = spark.sparkContext.textFile("fakefriends.csv")
```


```python
for i in lines.take(10):print(i)
```

    0,Will,33,385
    1,Jean-Luc,26,2
    2,Hugh,55,221
    3,Deanna,40,465
    4,Quark,68,21
    5,Weyoun,59,318
    6,Gowron,37,220
    7,Will,54,307
    8,Jadzia,38,380
    9,Hugh,27,181


As we can se, input file does not have header nor doest it have any strucure of columns. Firstly, we have to format the input


```python
def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), \
               age=int(fields[2]), numFriends=int(fields[3]))
```


```python
linesFormatted = lines.map(mapper)
```


```python
for i in linesFormatted.take(10):print(i)
```

    Row(ID=0, name="b'Will'", age=33, numFriends=385)
    Row(ID=1, name="b'Jean-Luc'", age=26, numFriends=2)
    Row(ID=2, name="b'Hugh'", age=55, numFriends=221)
    Row(ID=3, name="b'Deanna'", age=40, numFriends=465)
    Row(ID=4, name="b'Quark'", age=68, numFriends=21)
    Row(ID=5, name="b'Weyoun'", age=59, numFriends=318)
    Row(ID=6, name="b'Gowron'", age=37, numFriends=220)
    Row(ID=7, name="b'Will'", age=54, numFriends=307)
    Row(ID=8, name="b'Jadzia'", age=38, numFriends=380)
    Row(ID=9, name="b'Hugh'", age=27, numFriends=181)


As shown above, we have an RDD, but to make it simple we will solve the problem using dataframes. For that purpose, we convert the RDD to dataframe.
<strong>It is important to note that, to improve performance, as we are going for various tasks, we will use the cache function.</strong>


```python
peopleDf= linesFormatted.toDF().cache()
```


```python
peopleDf.show()
```

    +---+-----------+---+----------+
    | ID|       name|age|numFriends|
    +---+-----------+---+----------+
    |  0|    b'Will'| 33|       385|
    |  1|b'Jean-Luc'| 26|         2|
    |  2|    b'Hugh'| 55|       221|
    |  3|  b'Deanna'| 40|       465|
    |  4|   b'Quark'| 68|        21|
    |  5|  b'Weyoun'| 59|       318|
    |  6|  b'Gowron'| 37|       220|
    |  7|    b'Will'| 54|       307|
    |  8|  b'Jadzia'| 38|       380|
    |  9|    b'Hugh'| 27|       181|
    | 10|     b'Odo'| 53|       191|
    | 11|     b'Ben'| 57|       372|
    | 12|   b'Keiko'| 54|       253|
    | 13|b'Jean-Luc'| 56|       444|
    | 14|    b'Hugh'| 43|        49|
    | 15|     b'Rom'| 36|        49|
    | 16|  b'Weyoun'| 22|       323|
    | 17|     b'Odo'| 35|        13|
    | 18|b'Jean-Luc'| 45|       455|
    | 19|  b'Geordi'| 60|       246|
    +---+-----------+---+----------+
    only showing top 20 rows
    


we will solve it by SQL means


```python
peopleDf.createOrReplaceTempView("people")
```

An now the query to gett the people aged between 13 & 19 (teenagers) is so easy :


```python
teenagers = spark.sql("select count(age) as number_of_teenagers from people where age>=13 and age <= 19")
```


```python
teenagers.show()
```

    +-------------------+
    |number_of_teenagers|
    +-------------------+
    |                 19|
    +-------------------+
    


Now, we will have to get the number of people by age


```python
peopleDf.groupBy("age").count().orderBy("age").show()
```

    +---+-----+
    |age|count|
    +---+-----+
    | 18|    8|
    | 19|   11|
    | 20|    5|
    | 21|    8|
    | 22|    7|
    | 23|   10|
    | 24|    5|
    | 25|   11|
    | 26|   17|
    | 27|    8|
    | 28|   10|
    | 29|   12|
    | 30|   11|
    | 31|    8|
    | 32|   11|
    | 33|   12|
    | 34|    6|
    | 35|    8|
    | 36|   10|
    | 37|    9|
    +---+-----+
    only showing top 20 rows
    



```python
spark.stop()
```

4. <strong>Implement the word count solution with dataframes</strong>


```python
spark = SparkSession.builder.appName("WordCount").getOrCreate()
```

We read the book file as a dataframe


```python
book = spark.read.text("Book")
```

We will split the text using a regular expression that extracts words.
"explode" function works as "flatMap" function, using a defined function for each of the elements (the regular expression)



```python
words = book.select(func.explode(func.split(book.value, "\\W+")).alias("word"))
words.filter(words.word != "")
```




    DataFrame[word: string]




```python
words.show()
```

    +----------+
    |      word|
    +----------+
    |      Self|
    |Employment|
    |  Building|
    |        an|
    |  Internet|
    |  Business|
    |        of|
    |       One|
    | Achieving|
    | Financial|
    |       and|
    |  Personal|
    |   Freedom|
    |   through|
    |         a|
    | Lifestyle|
    |Technology|
    |  Business|
    |        By|
    |     Frank|
    +----------+
    only showing top 20 rows
    


As we can see, we have a dataframe composed of just one column (word). We will have to create a function to apply a lower case function to all words)


```python
lowercaseWords = words.select(func.lower(words.word).alias("word"))
```


```python
lowercaseWords.show()
```

    +----------+
    |      word|
    +----------+
    |      self|
    |employment|
    |  building|
    |        an|
    |  internet|
    |  business|
    |        of|
    |       one|
    | achieving|
    | financial|
    |       and|
    |  personal|
    |   freedom|
    |   through|
    |         a|
    | lifestyle|
    |technology|
    |  business|
    |        by|
    |     frank|
    +----------+
    only showing top 20 rows
    


Now, we will group by word to count its appearances


```python
lowercaseWords.groupBy("word").count().orderBy(col("count").desc()).show()
```


    ---------------------------------------------------------------------------

    NameError                                 Traceback (most recent call last)

    <ipython-input-33-576afd2d4580> in <module>
    ----> 1 lowercaseWords.groupBy("word").count().orderBy(col("count").desc()).show()
    

    NameError: name 'lowercaseWords' is not defined



```python
spark.stop()
```

5. <strong>Get the minimum temperature by station</strong>


```python
from pyspark.sql import SparkSession

```


```python
spark = SparkSession.builder.appName("minimumTemperatureByStation").getOrCreate()
```


```python
minTemperaturesFile = spark.read.csv("1800.csv")
```


```python
minTemperaturesFile.show()
```

    +-----------+--------+----+----+----+----+---+----+
    |        _c0|     _c1| _c2| _c3| _c4| _c5|_c6| _c7|
    +-----------+--------+----+----+----+----+---+----+
    |ITE00100554|18000101|TMAX| -75|null|null|  E|null|
    |ITE00100554|18000101|TMIN|-148|null|null|  E|null|
    |GM000010962|18000101|PRCP|   0|null|null|  E|null|
    |EZE00100082|18000101|TMAX| -86|null|null|  E|null|
    |EZE00100082|18000101|TMIN|-135|null|null|  E|null|
    |ITE00100554|18000102|TMAX| -60|null|   I|  E|null|
    |ITE00100554|18000102|TMIN|-125|null|null|  E|null|
    |GM000010962|18000102|PRCP|   0|null|null|  E|null|
    |EZE00100082|18000102|TMAX| -44|null|null|  E|null|
    |EZE00100082|18000102|TMIN|-130|null|null|  E|null|
    |ITE00100554|18000103|TMAX| -23|null|null|  E|null|
    |ITE00100554|18000103|TMIN| -46|null|   I|  E|null|
    |GM000010962|18000103|PRCP|   4|null|null|  E|null|
    |EZE00100082|18000103|TMAX| -10|null|null|  E|null|
    |EZE00100082|18000103|TMIN| -73|null|null|  E|null|
    |ITE00100554|18000104|TMAX|   0|null|null|  E|null|
    |ITE00100554|18000104|TMIN| -13|null|null|  E|null|
    |GM000010962|18000104|PRCP|   0|null|null|  E|null|
    |EZE00100082|18000104|TMAX| -55|null|null|  E|null|
    |EZE00100082|18000104|TMIN| -74|null|null|  E|null|
    +-----------+--------+----+----+----+----+---+----+
    only showing top 20 rows
    


As we can see, columns don't have a descriptive name, so we are going to define a schema for all the necessary columns


```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
```


```python
schema = StructType([ \
                     StructField("stationID", StringType(), True), \
                     StructField("date", IntegerType(), True), \
                     StructField("measure_type", StringType(), True), \
                     StructField("temperature", FloatType(), True)])
```


```python
df = spark.read.schema(schema).csv("1800.csv")
df.printSchema()
```

    root
     |-- stationID: string (nullable = true)
     |-- date: integer (nullable = true)
     |-- measure_type: string (nullable = true)
     |-- temperature: float (nullable = true)
    



```python
df.show()
```

    +-----------+--------+------------+-----------+
    |  stationID|    date|measure_type|temperature|
    +-----------+--------+------------+-----------+
    |ITE00100554|18000101|        TMAX|      -75.0|
    |ITE00100554|18000101|        TMIN|     -148.0|
    |GM000010962|18000101|        PRCP|        0.0|
    |EZE00100082|18000101|        TMAX|      -86.0|
    |EZE00100082|18000101|        TMIN|     -135.0|
    |ITE00100554|18000102|        TMAX|      -60.0|
    |ITE00100554|18000102|        TMIN|     -125.0|
    |GM000010962|18000102|        PRCP|        0.0|
    |EZE00100082|18000102|        TMAX|      -44.0|
    |EZE00100082|18000102|        TMIN|     -130.0|
    |ITE00100554|18000103|        TMAX|      -23.0|
    |ITE00100554|18000103|        TMIN|      -46.0|
    |GM000010962|18000103|        PRCP|        4.0|
    |EZE00100082|18000103|        TMAX|      -10.0|
    |EZE00100082|18000103|        TMIN|      -73.0|
    |ITE00100554|18000104|        TMAX|        0.0|
    |ITE00100554|18000104|        TMIN|      -13.0|
    |GM000010962|18000104|        PRCP|        0.0|
    |EZE00100082|18000104|        TMAX|      -55.0|
    |EZE00100082|18000104|        TMIN|      -74.0|
    +-----------+--------+------------+-----------+
    only showing top 20 rows
    


Now we will create the solution for the problem


```python
df.filter(df.measure_type=="TMIN").select(df.stationID, df.temperature).show()
```

    +-----------+-----------+
    |  stationID|temperature|
    +-----------+-----------+
    |ITE00100554|     -148.0|
    |EZE00100082|     -135.0|
    |ITE00100554|     -125.0|
    |EZE00100082|     -130.0|
    |ITE00100554|      -46.0|
    |EZE00100082|      -73.0|
    |ITE00100554|      -13.0|
    |EZE00100082|      -74.0|
    |ITE00100554|       -6.0|
    |EZE00100082|      -58.0|
    |ITE00100554|       13.0|
    |EZE00100082|      -57.0|
    |ITE00100554|       10.0|
    |EZE00100082|      -50.0|
    |ITE00100554|       14.0|
    |EZE00100082|      -31.0|
    |ITE00100554|       23.0|
    |EZE00100082|      -46.0|
    |ITE00100554|       31.0|
    |EZE00100082|      -75.0|
    +-----------+-----------+
    only showing top 20 rows
    



```python
df.filter(df.measure_type=="TMIN").select(df.stationID, df.temperature).groupBy(df.stationID).min("temperature").show()
```

    +-----------+----------------+
    |  stationID|min(temperature)|
    +-----------+----------------+
    |ITE00100554|          -148.0|
    |EZE00100082|          -135.0|
    +-----------+----------------+
    



```python
spark.stop()
```

6. <strong>Get the total amount spent by customers using dataframes</strong>


```python
spark = SparkSession.builder.appName("totalSpentCustomers").getOrCreate()
```


```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
```

We define the inout schema


```python
schema = StructType([ \
                     StructField("customerID", StringType(), True), \
                     StructField("orderID", IntegerType(), True), \
                     StructField("amount", FloatType(), True)])
```


```python
df_customers = spark.read.schema(schema).csv("customer-orders.csv")
df_customers.printSchema()
```

    root
     |-- customerID: string (nullable = true)
     |-- orderID: integer (nullable = true)
     |-- amount: float (nullable = true)
    



```python
df_customers.show()
```

    +----------+-------+------+
    |customerID|orderID|amount|
    +----------+-------+------+
    |        44|   8602| 37.19|
    |        35|   5368| 65.89|
    |         2|   3391| 40.64|
    |        47|   6694| 14.98|
    |        29|    680| 13.08|
    |        91|   8900| 24.59|
    |        70|   3959| 68.68|
    |        85|   1733| 28.53|
    |        53|   9900| 83.55|
    |        14|   1505|  4.32|
    |        51|   3378|  19.8|
    |        42|   6926| 57.77|
    |         2|   4424| 55.77|
    |        79|   9291| 33.17|
    |        50|   3901| 23.57|
    |        20|   6633|  6.49|
    |        15|   6148| 65.53|
    |        44|   8331| 99.19|
    |         5|   3505| 64.18|
    |        48|   5539| 32.42|
    +----------+-------+------+
    only showing top 20 rows
    


They are asking to sum all amounts grouped by customer


```python
from pyspark.sql import functions as func
from pyspark.sql.functions import col, asc,desc
df_customers.select("customerID", "amount").groupBy("customerID").agg(func.round(func.sum("amount"), 2).alias("total_amount")).orderBy(col("total_amount").desc()).show()
```

    +----------+------------+
    |customerID|total_amount|
    +----------+------------+
    |        68|     6375.45|
    |        73|      6206.2|
    |        39|     6193.11|
    |        54|     6065.39|
    |        71|     5995.66|
    |         2|     5994.59|
    |        97|     5977.19|
    |        46|     5963.11|
    |        42|     5696.84|
    |        59|     5642.89|
    |        41|     5637.62|
    |         0|     5524.95|
    |         8|     5517.24|
    |        85|     5503.43|
    |        61|     5497.48|
    |        32|     5496.05|
    |        58|     5437.73|
    |        63|     5415.15|
    |        15|     5413.51|
    |         6|     5397.88|
    +----------+------------+
    only showing top 20 rows
    



```python
spark.stop()
```

7. <strong>sort all movies by popularity in one line</strong>


```python
spark = SparkSession.builder.appName("moviePopularity").getOrCreate()
```

We define a schema for the input file


```python
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
schema = StructType([\
                    StructField("userID", IntegerType(), True),\
                    StructField("movieID", IntegerType(), True),\
                    StructField("rating", IntegerType(), True),\
                    StructField("timestamp", LongType(), True)])
```


```python
moviesDf = spark.read.option("sep", "\t").schema(schema).csv("u.data")
```


```python
moviesDf.show()
```

    +------+-------+------+---------+
    |userID|movieID|rating|timestamp|
    +------+-------+------+---------+
    |   196|    242|     3|881250949|
    |   186|    302|     3|891717742|
    |    22|    377|     1|878887116|
    |   244|     51|     2|880606923|
    |   166|    346|     1|886397596|
    |   298|    474|     4|884182806|
    |   115|    265|     2|881171488|
    |   253|    465|     5|891628467|
    |   305|    451|     3|886324817|
    |     6|     86|     3|883603013|
    |    62|    257|     2|879372434|
    |   286|   1014|     5|879781125|
    |   200|    222|     5|876042340|
    |   210|     40|     3|891035994|
    |   224|     29|     3|888104457|
    |   303|    785|     3|879485318|
    |   122|    387|     5|879270459|
    |   194|    274|     2|879539794|
    |   291|   1042|     4|874834944|
    |   234|   1184|     2|892079237|
    +------+-------+------+---------+
    only showing top 20 rows
    



```python
moviesDf.groupBy("movieID").agg(func.count("movieID").alias("timesWatched")).orderBy(col("timesWatched").desc()).show()
```

    +-------+------------+
    |movieID|timesWatched|
    +-------+------------+
    |     50|         583|
    |    258|         509|
    |    100|         508|
    |    181|         507|
    |    294|         485|
    |    286|         481|
    |    288|         478|
    |      1|         452|
    |    300|         431|
    |    121|         429|
    |    174|         420|
    |    127|         413|
    |     56|         394|
    |      7|         392|
    |     98|         390|
    |    237|         384|
    |    117|         378|
    |    172|         367|
    |    222|         365|
    |    204|         350|
    +-------+------------+
    only showing top 20 rows
    


Or even, using with a simple line:


```python
moviesDf.groupBy("movieID").count().orderBy(func.desc("count")).show()
```

    +-------+-----+
    |movieID|count|
    +-------+-----+
    |     50|  583|
    |    258|  509|
    |    100|  508|
    |    181|  507|
    |    294|  485|
    |    286|  481|
    |    288|  478|
    |      1|  452|
    |    300|  431|
    |    121|  429|
    |    174|  420|
    |    127|  413|
    |     56|  394|
    |      7|  392|
    |     98|  390|
    |    237|  384|
    |    117|  378|
    |    172|  367|
    |    222|  365|
    |    204|  350|
    +-------+-----+
    only showing top 20 rows
    



```python
spark.stop()
```

8. <strong>Get the most popular movies including its names (using u.ITEM file, which contains movie names)</strong>


```python
spark = SparkSession.builder.appName("PopularMovies").getOrCreate()
```


```python
movieNames = spark.sparkContext.textFile("u.item")
```


```python
for i in movieNames.take(10):print(i)
```

    1|Toy Story (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Toy%20Story%20(1995)|0|0|0|1|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0
    2|GoldenEye (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?GoldenEye%20(1995)|0|1|1|0|0|0|0|0|0|0|0|0|0|0|0|0|1|0|0
    3|Four Rooms (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Four%20Rooms%20(1995)|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|0|1|0|0
    4|Get Shorty (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Get%20Shorty%20(1995)|0|1|0|0|0|1|0|0|1|0|0|0|0|0|0|0|0|0|0
    5|Copycat (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Copycat%20(1995)|0|0|0|0|0|0|1|0|1|0|0|0|0|0|0|0|1|0|0
    6|Shanghai Triad (Yao a yao yao dao waipo qiao) (1995)|01-Jan-1995||http://us.imdb.com/Title?Yao+a+yao+yao+dao+waipo+qiao+(1995)|0|0|0|0|0|0|0|0|1|0|0|0|0|0|0|0|0|0|0
    7|Twelve Monkeys (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Twelve%20Monkeys%20(1995)|0|0|0|0|0|0|0|0|1|0|0|0|0|0|0|1|0|0|0
    8|Babe (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Babe%20(1995)|0|0|0|0|1|1|0|0|1|0|0|0|0|0|0|0|0|0|0
    9|Dead Man Walking (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Dead%20Man%20Walking%20(1995)|0|0|0|0|0|0|0|0|1|0|0|0|0|0|0|0|0|0|0
    10|Richard III (1995)|22-Jan-1996||http://us.imdb.com/M/title-exact?Richard%20III%20(1995)|0|0|0|0|0|0|0|0|1|0|0|0|0|0|0|0|0|1|0


As we can see from the RDD shown above, we have to git its first two columns (1.movieID, 2. MovieName). Then, we should join this dataframe with the one of the previous exercise to add "movieName" column). We could implement the solution by using one of the next methods:
1) broadcasting the movieNames dataframe accross all nodes so as to make every node match each task with its movieName.

2) join dataframe from the previuos section with name dataframe

<strong>Solution 1) By using broadcast</strong>


```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs
from pyspark.sql.functions import col
```

We will create a dictiones with the next structure {movieID:movieName}. For that purpose, we will create a function to be used later, as a udf function


```python
import codecs

def loadMovieNames():
    movieNames = {}
    with codecs.open("u.item", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

```


```python
nameDict = spark.sparkContext.broadcast(loadMovieNames())
```

Now, wil will read the file containing movies and its voted times:

Then, we will define the schema for that file (the same from the exercise above)


```python
schema = StructType([\
                    StructField("userID", IntegerType(), True), \
                    StructField("movieID", IntegerType(), True), \
                    StructField("rating", IntegerType(), True), \
                    StructField("timestamp", LongType(), True)])
```


```python
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("u.data")
```


```python
moviesDF.show()
```

    +------+-------+------+---------+
    |userID|movieID|rating|timestamp|
    +------+-------+------+---------+
    |   196|    242|     3|881250949|
    |   186|    302|     3|891717742|
    |    22|    377|     1|878887116|
    |   244|     51|     2|880606923|
    |   166|    346|     1|886397596|
    |   298|    474|     4|884182806|
    |   115|    265|     2|881171488|
    |   253|    465|     5|891628467|
    |   305|    451|     3|886324817|
    |     6|     86|     3|883603013|
    |    62|    257|     2|879372434|
    |   286|   1014|     5|879781125|
    |   200|    222|     5|876042340|
    |   210|     40|     3|891035994|
    |   224|     29|     3|888104457|
    |   303|    785|     3|879485318|
    |   122|    387|     5|879270459|
    |   194|    274|     2|879539794|
    |   291|   1042|     4|874834944|
    |   234|   1184|     2|892079237|
    +------+-------+------+---------+
    only showing top 20 rows
    



```python
movieCounts= moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))
```


```python
movieCounts.show()
```

    +-------+-----+
    |movieID|count|
    +-------+-----+
    |     50|  583|
    |    258|  509|
    |    100|  508|
    |    181|  507|
    |    294|  485|
    |    286|  481|
    |    288|  478|
    |      1|  452|
    |    300|  431|
    |    121|  429|
    |    174|  420|
    |    127|  413|
    |     56|  394|
    |      7|  392|
    |     98|  390|
    |    237|  384|
    |    117|  378|
    |    172|  367|
    |    222|  365|
    |    204|  350|
    +-------+-----+
    only showing top 20 rows
    


Now, we will create a UDF (user_defined function) to search movie names from the broadcasted dictionary


```python
def lookupName(movieID):
    # the boradcasted dictionary
    return nameDict.value[movieID]

lookupNameUDF = func.udf(lookupName)
```

Now, after having created the UDF, we have to add the column of the movie title to our dataframe (movieCounts)


```python
moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))
```


```python
moviesWithNames.show()
```

    +-------+-----+--------------------+
    |movieID|count|          movieTitle|
    +-------+-----+--------------------+
    |     50|  583|    Star Wars (1977)|
    |    258|  509|      Contact (1997)|
    |    100|  508|        Fargo (1996)|
    |    181|  507|Return of the Jed...|
    |    294|  485|    Liar Liar (1997)|
    |    286|  481|English Patient, ...|
    |    288|  478|       Scream (1996)|
    |      1|  452|    Toy Story (1995)|
    |    300|  431|Air Force One (1997)|
    |    121|  429|Independence Day ...|
    |    174|  420|Raiders of the Lo...|
    |    127|  413|Godfather, The (1...|
    |     56|  394| Pulp Fiction (1994)|
    |      7|  392|Twelve Monkeys (1...|
    |     98|  390|Silence of the La...|
    |    237|  384|Jerry Maguire (1996)|
    |    117|  378|    Rock, The (1996)|
    |    172|  367|Empire Strikes Ba...|
    |    222|  365|Star Trek: First ...|
    |    204|  350|Back to the Futur...|
    +-------+-----+--------------------+
    only showing top 20 rows
    


<strong>Solution 2) By using join</strong>

We consider the problem from the beginning, so we read input files


```python
moviesDF = spark.read.option("sep", "\t").schema(schema).csv("u.data")
```

We find out the most rated movies


```python
movieCounts= moviesDF.groupBy("movieID").count().orderBy(func.desc("count"))
```

We define the schema of the movie file with names


```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
```


```python
schema = StructType([\
                    StructField("movieID", IntegerType(), True), \
                    StructField("movieName", StringType(), True)])
```


```python
movieNames = spark.read.option("sep", "|").schema(schema).csv("u.item")
```


```python
movieNames.show()
```

    +-------+--------------------+
    |movieID|           movieName|
    +-------+--------------------+
    |      1|    Toy Story (1995)|
    |      2|    GoldenEye (1995)|
    |      3|   Four Rooms (1995)|
    |      4|   Get Shorty (1995)|
    |      5|      Copycat (1995)|
    |      6|Shanghai Triad (Y...|
    |      7|Twelve Monkeys (1...|
    |      8|         Babe (1995)|
    |      9|Dead Man Walking ...|
    |     10|  Richard III (1995)|
    |     11|Seven (Se7en) (1995)|
    |     12|Usual Suspects, T...|
    |     13|Mighty Aphrodite ...|
    |     14|  Postino, Il (1994)|
    |     15|Mr. Holland's Opu...|
    |     16|French Twist (Gaz...|
    |     17|From Dusk Till Da...|
    |     18|White Balloon, Th...|
    |     19|Antonia's Line (1...|
    |     20|Angels and Insect...|
    +-------+--------------------+
    only showing top 20 rows
    



```python
movieCounts.show()
```

    +-------+-----+
    |movieID|count|
    +-------+-----+
    |    405|  737|
    |    655|  685|
    |     13|  636|
    |    450|  540|
    |    276|  518|
    |    416|  493|
    |    537|  490|
    |    303|  484|
    |    234|  480|
    |    393|  448|
    |    181|  435|
    |    279|  434|
    |    429|  414|
    |    846|  405|
    |      7|  403|
    |     94|  400|
    |    682|  399|
    |    308|  397|
    |    293|  388|
    |     92|  388|
    +-------+-----+
    only showing top 20 rows
    



```python
movieCounts.printSchema()
```

    root
     |-- movieID: integer (nullable = true)
     |-- count: long (nullable = false)
    



```python
moviesJoin = movieNames.join(movieCounts, movieNames.movieID == movieCounts.movieID, 'inner').select(movieCounts.movieID,movieNames.movieName, "count").orderBy(func.desc("count"))
```


```python
moviesJoin.show()
```

    +-------+--------------------+-----+
    |movieID|           movieName|count|
    +-------+--------------------+-----+
    |    405|Mission: Impossib...|  737|
    |    655|  Stand by Me (1986)|  685|
    |     13|Mighty Aphrodite ...|  636|
    |    450|Star Trek V: The ...|  540|
    |    276|Leaving Las Vegas...|  518|
    |    416|   Old Yeller (1957)|  493|
    |    537|My Own Private Id...|  490|
    |    303|  Ulee's Gold (1997)|  484|
    |    234|         Jaws (1975)|  480|
    |    393|Mrs. Doubtfire (1...|  448|
    |    181|Return of the Jed...|  435|
    |    279|Once Upon a Time....|  434|
    |    429|Day the Earth Sto...|  414|
    |    846|To Gillian on Her...|  405|
    |      7|Twelve Monkeys (1...|  403|
    |     94|   Home Alone (1990)|  400|
    |    682|I Know What You D...|  399|
    |    308|FairyTale: A True...|  397|
    |     92| True Romance (1993)|  388|
    |    293|Donnie Brasco (1997)|  388|
    +-------+--------------------+-----+
    only showing top 20 rows
    

