DATAFRAME EXERCISES FOR CCA175 CLOUDERA PYSPARK DEVELOPER
=============================================
Taking into account this data model:

![GitHub Logo](data_model.jpg)

We will show the content of all the tables in the data model

**Categories**
```python
 categories = sc.textFile("/public/retail_db/categories")
 for i in categories.take(10):print(i)
... 
1,2,Football                                                                    
2,2,Soccer
3,2,Baseball & Softball
4,2,Basketball
5,2,Lacrosse
6,2,Tennis & Racquet
7,2,Hockey
8,2,More Sports
9,3,Cardio Equipment
10,3,Strength Training
```

**Customers**
```python
>>> customers = sc.textFile("/public/retail_db/customers")
>>> for i in customers.take(10):print(i)
... 
1,Richard,Hernandez,XXXXXXXXX,XXXXXXXXX,6303 Heather Plaza,Brownsville,TX,78521
2,Mary,Barrett,XXXXXXXXX,XXXXXXXXX,9526 Noble Embers Ridge,Littleton,CO,80126
3,Ann,Smith,XXXXXXXXX,XXXXXXXXX,3422 Blue Pioneer Bend,Caguas,PR,00725
4,Mary,Jones,XXXXXXXXX,XXXXXXXXX,8324 Little Common,San Marcos,CA,92069
5,Robert,Hudson,XXXXXXXXX,XXXXXXXXX,"10 Crystal River Mall ",Caguas,PR,00725
6,Mary,Smith,XXXXXXXXX,XXXXXXXXX,3151 Sleepy Quail Promenade,Passaic,NJ,07055
7,Melissa,Wilcox,XXXXXXXXX,XXXXXXXXX,9453 High Concession,Caguas,PR,00725
8,Megan,Smith,XXXXXXXXX,XXXXXXXXX,3047 Foggy Forest Plaza,Lawrence,MA,01841
9,Mary,Perez,XXXXXXXXX,XXXXXXXXX,3616 Quaking Street,Caguas,PR,00725
10,Melissa,Smith,XXXXXXXXX,XXXXXXXXX,8598 Harvest Beacon Plaza,Stafford,VA,22554
```

**Departments**
```python
>>> departments = sc.textFile("/public/retail_db/departments")
>>> for i in departments.take(10):print(i)
... 
2,Fitness
3,Footwear
4,Apparel
5,Golf
6,Outdoors
7,Fan Shop
>>> 

```

**Order Items**
```python
>>> orderItems = sc.textFile("/public/retail_db/order_items")
>>> for i in orderItems.take(10):print(i)
... 
1,1,957,1,299.98,299.98
2,2,1073,1,199.99,199.99
3,2,502,5,250.0,50.0
4,2,403,1,129.99,129.99
5,4,897,2,49.98,24.99
6,4,365,5,299.95,59.99
7,4,502,3,150.0,50.0
8,4,1014,4,199.92,49.98
9,5,957,1,299.98,299.98
10,5,365,5,299.95,59.99
>>> 
```

**Orders**
```python
>>> orders = sc.textFile("/public/retail_db/orders")
>>> for i in orders.take(10):print(i)
... 
1,2013-07-25 00:00:00.0,11599,CLOSED
2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT
3,2013-07-25 00:00:00.0,12111,COMPLETE
4,2013-07-25 00:00:00.0,8827,CLOSED
5,2013-07-25 00:00:00.0,11318,COMPLETE
6,2013-07-25 00:00:00.0,7130,COMPLETE
7,2013-07-25 00:00:00.0,4530,COMPLETE
8,2013-07-25 00:00:00.0,2911,PROCESSING
9,2013-07-25 00:00:00.0,5657,PENDING_PAYMENT
10,2013-07-25 00:00:00.0,5648,PENDING_PAYMENT

```

**Products**
```python
>>> products = sc.textFile("/public/retail_db/products")
>>> for i in products.take(10):print(i)
... 
1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy
2,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
3,2,Under Armour Men's Renegade D Mid Football Cl,,89.99,http://images.acmesports.sports/Under+Armour+Men%27s+Renegade+D+Mid+Football+Cleat
4,2,Under Armour Men's Renegade D Mid Football Cl,,89.99,http://images.acmesports.sports/Under+Armour+Men%27s+Renegade+D+Mid+Football+Cleat
5,2,Riddell Youth Revolution Speed Custom Footbal,,199.99,http://images.acmesports.sports/Riddell+Youth+Revolution+Speed+Custom+Football+Helmet
6,2,Jordan Men's VI Retro TD Football Cleat,,134.99,http://images.acmesports.sports/Jordan+Men%27s+VI+Retro+TD+Football+Cleat
7,2,Schutt Youth Recruit Hybrid Custom Football H,,99.99,http://images.acmesports.sports/Schutt+Youth+Recruit+Hybrid+Custom+Football+Helmet+2014
8,2,Nike Men's Vapor Carbon Elite TD Football Cle,,129.99,http://images.acmesports.sports/Nike+Men%27s+Vapor+Carbon+Elite+TD+Football+Cleat
9,2,Nike Adult Vapor Jet 3.0 Receiver Gloves,,50.0,http://images.acmesports.sports/Nike+Adult+Vapor+Jet+3.0+Receiver+Gloves
10,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
>>> 

```


## EXERCISES USING DATAFRAMES

**Exercise 1**
Get daily revenue per product considering COMPLETE and CLOSED orders. PRODUCTS have to be read from the local file system. Dataframe need to be created.
Data need to be sorted in ascending order by date and then, descending order by revenue computed for each product and date.

We launch pyspark on the console
```python
   pyspark --master yarn --conf spark.ui.port=12562 --executor-memory 2G --num-executors 1
```

We show what we have using the sql context (it will return a dataframe):
```python
sqlContext.sql("select * from carlos_sanchez_db_orc.orders").show(10)
+--------+--------------------+-----------------+---------------+
|order_id|          order_date|order_customer_id|   order_status|
+--------+--------------------+-----------------+---------------+
|       1|2013-07-25 00:00:...|            11599|         CLOSED|
|       2|2013-07-25 00:00:...|              256|PENDING_PAYMENT|
|       3|2013-07-25 00:00:...|            12111|       COMPLETE|
|       4|2013-07-25 00:00:...|             8827|         CLOSED|
|       5|2013-07-25 00:00:...|            11318|       COMPLETE|
|       6|2013-07-25 00:00:...|             7130|       COMPLETE|
|       7|2013-07-25 00:00:...|             4530|       COMPLETE|
|       8|2013-07-25 00:00:...|             2911|     PROCESSING|
|       9|2013-07-25 00:00:...|             5657|PENDING_PAYMENT|
|      10|2013-07-25 00:00:...|             5648|PENDING_PAYMENT|
+--------+--------------------+-----------------+---------------+
only showing top 10 rows

```

As the problem statement mentions, we have to read the tables in RDD format to convert them to a dataframe data structure.
```python
from pyspark.sql import Row
ordersRDD = sc.textFile("/public/retail_db/orders")
```
Now that we have read the data in RDD format, we have to convert it to dataframe:
```python
ordersDF = ordersRDD.\
map(lambda o: Row(order_id=int(o.split(",")[0]), order_date=o.split(",")[1], order_customer_id=int(o.split(",")[2]), order_status=o.split(",")[3])).toDF()
>>> ordersDF.show()
+-----------------+--------------------+--------+---------------+
|order_customer_id|          order_date|order_id|   order_status|
+-----------------+--------------------+--------+---------------+
|            11599|2013-07-25 00:00:...|       1|         CLOSED|
|              256|2013-07-25 00:00:...|       2|PENDING_PAYMENT|
|            12111|2013-07-25 00:00:...|       3|       COMPLETE|
|             8827|2013-07-25 00:00:...|       4|         CLOSED|
|            11318|2013-07-25 00:00:...|       5|       COMPLETE|
|             7130|2013-07-25 00:00:...|       6|       COMPLETE|
|             4530|2013-07-25 00:00:...|       7|       COMPLETE|
|             2911|2013-07-25 00:00:...|       8|     PROCESSING|
|             5657|2013-07-25 00:00:...|       9|PENDING_PAYMENT|
|             5648|2013-07-25 00:00:...|      10|PENDING_PAYMENT|
|              918|2013-07-25 00:00:...|      11| PAYMENT_REVIEW|
|             1837|2013-07-25 00:00:...|      12|         CLOSED|
|             9149|2013-07-25 00:00:...|      13|PENDING_PAYMENT|
|             9842|2013-07-25 00:00:...|      14|     PROCESSING|
|             2568|2013-07-25 00:00:...|      15|       COMPLETE|
|             7276|2013-07-25 00:00:...|      16|PENDING_PAYMENT|
|             2667|2013-07-25 00:00:...|      17|       COMPLETE|
|             1205|2013-07-25 00:00:...|      18|         CLOSED|
|             9488|2013-07-25 00:00:...|      19|PENDING_PAYMENT|
|             9198|2013-07-25 00:00:...|      20|     PROCESSING|
+-----------------+--------------------+--------+---------------+
only showing top 20 rows

```
Now, in orther to execute queries against that table, we have to create a temp table:

```python
    ordersDF.registerTempTable("ordersDF_table")
```

Now, we have to get product data from the local file system (note that we use the "file:// " part to locate the file from the local file system:

```python
productsRaw = sc.textFile("file:///data/retail_db/products/part-00000")
>>> type(productsRaw)
<class 'pyspark.rdd.RDD'>
```
we show the content of the data read:
```python
>>> for i in productsRaw.take(2):print(i)
... 
1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy
2,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
```

We have the data in RDD format, so we will convert it to a dataframe:

```python
productsDF = productsRDD.\
map(lambda p: Row(product_id=int(p.split(",")[0]), product_name = p.split(",")[2])).toDF()
>>> productsDF.show()
+----------+--------------------+
|product_id|        product_name|
+----------+--------------------+
|         1|Quest Q64 10 FT. ...|
|         2|Under Armour Men'...|
|         3|Under Armour Men'...|
|         4|Under Armour Men'...|
|         5|Riddell Youth Rev...|
|         6|Jordan Men's VI R...|

```

Then, we register the table, as well, a temp table:
```python
productsDF.registerTempTable("products")
```

We check whether the dataframes are properly created:
```python
    >>> sqlContext.sql("show tables ").show()
    +-----------+-----------+
    |  tableName|isTemporary|
    +-----------+-----------+
    |   products|       true|
    |  customers|      false|
    |order_items|      false|
    |     orders|      false|
    +-----------+-----------+
```

Now we will show the tables in order to view which are the columns that we have to join the tables by
```python    
    >>> sqlContext.sql("select * from products").show()
    +----------+--------------------+
    |product_id|        product_name|
    +----------+--------------------+
    |         1|Quest Q64 10 FT. ...|
    |         2|Under Armour Men'...|
    |         3|Under Armour Men'...|
    |         4|Under Armour Men'...|
    |         5|Riddell Youth Rev...|
    |         6|Jordan Men's VI R...|
    |         7|Schutt Youth Recr...|
    |         8|Nike Men's Vapor ...|
    |         9|Nike Adult Vapor ...|
    |        10|Under Armour Men'...|
    |        11|Fitness Gear 300 ...|
    |        12|Under Armour Men'...|
    |        13|Under Armour Men'...|
    |        14|Quik Shade Summit...|
    |        15|Under Armour Kids...|
    |        16|Riddell Youth 360...|
    |        17|Under Armour Men'...|
    |        18|Reebok Men's Full...|
    |        19|Nike Men's Finger...|
    |        20|Under Armour Men'...|
    +----------+--------------------+
    only showing top 20 rows


    >>> sqlContext.sql("select * from orders").show()
    +--------+--------------------+-----------------+---------------+
    |order_id|          order_date|order_customer_id|   order_status|
    +--------+--------------------+-----------------+---------------+
    |       1|2013-07-25 00:00:...|            11599|         CLOSED|
    |       2|2013-07-25 00:00:...|              256|PENDING_PAYMENT|
    |       3|2013-07-25 00:00:...|            12111|       COMPLETE|
    |       4|2013-07-25 00:00:...|             8827|         CLOSED|
    |       5|2013-07-25 00:00:...|            11318|       COMPLETE|
    |       6|2013-07-25 00:00:...|             7130|       COMPLETE|
    |       7|2013-07-25 00:00:...|             4530|       COMPLETE|
    |       8|2013-07-25 00:00:...|             2911|     PROCESSING|
    |       9|2013-07-25 00:00:...|             5657|PENDING_PAYMENT|
    |      10|2013-07-25 00:00:...|             5648|PENDING_PAYMENT|
    |      11|2013-07-25 00:00:...|              918| PAYMENT_REVIEW|
    |      12|2013-07-25 00:00:...|             1837|         CLOSED|
    |      13|2013-07-25 00:00:...|             9149|PENDING_PAYMENT|
    |      14|2013-07-25 00:00:...|             9842|     PROCESSING|
    |      15|2013-07-25 00:00:...|             2568|       COMPLETE|
    |      16|2013-07-25 00:00:...|             7276|PENDING_PAYMENT|
    |      17|2013-07-25 00:00:...|             2667|       COMPLETE|
    |      18|2013-07-25 00:00:...|             1205|         CLOSED|
    |      19|2013-07-25 00:00:...|             9488|PENDING_PAYMENT|
    |      20|2013-07-25 00:00:...|             9198|     PROCESSING|
    +--------+--------------------+-----------------+---------------+
    only showing top 20 rows

    >>> sqlContext.sql("select * from order_items").show()
    +-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
    |order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
    +-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
    |            1|                  1|                  957|                  1|             299.98|                  299.98|
    |            2|                  2|                 1073|                  1|             199.99|                  199.99|
    |            3|                  2|                  502|                  5|              250.0|                    50.0|
    |            4|                  2|                  403|                  1|             129.99|                  129.99|
    |            5|                  4|                  897|                  2|              49.98|                   24.99|
    |            6|                  4|                  365|                  5|             299.95|                   59.99|
    |            7|                  4|                  502|                  3|              150.0|                    50.0|
    |            8|                  4|                 1014|                  4|             199.92|                   49.98|
    |            9|                  5|                  957|                  1|             299.98|                  299.98|
    |           10|                  5|                  365|                  5|             299.95|                   59.99|
    |           11|                  5|                 1014|                  2|              99.96|                   49.98|
    |           12|                  5|                  957|                  1|             299.98|                  299.98|
    |           13|                  5|                  403|                  1|             129.99|                  129.99|
    |           14|                  7|                 1073|                  1|             199.99|                  199.99|
    |           15|                  7|                  957|                  1|             299.98|                  299.98|
    |           16|                  7|                  926|                  5|              79.95|                   15.99|
    |           17|                  8|                  365|                  3|             179.97|                   59.99|
    |           18|                  8|                  365|                  5|             299.95|                   59.99|
    |           19|                  8|                 1014|                  4|             199.92|                   49.98|
    |           20|                  8|                  502|                  1|               50.0|                    50.0|
    +-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
    only showing top 20 rows
```

From order_items table we will need "order_item_product_id" to join with "product" table( but, in the end, we don't need that column. In spite of using it, we need the product_name column from product table) and order_item_subtotal to get the revenue. So, the steps are:

1.We need to join order_item and product to get product name
2.Join order_items with orders because we need the order_date column from orders table. In addition, we need to filter the rows in COMPLETE or CLOSED state.
We want to have total revenue by date and product, so we have to group the results using the GROUP BY function (GROUP BY o.order_date, p.product_name. In addition, we use sum(oi.order_item_subtotal) because we need daily revenue per product.


```python
>>> sqlContext.sql("SELECT o.order_date, p.product_name, sum(oi.order_item_subtotal) daily_revenue_per_product \
        ... FROM orders o JOIN order_items oi \
        ... ON o.order_id = oi.order_item_order_id \
        ... JOIN products p \
        ... ON p.product_id = oi.order_item_product_id \
        ... WHERE o.order_status IN ('COMPLETE', 'CLOSED') \
        ... GROUP BY o.order_date, p.product_name \
        ... ORDER BY o.order_date, daily_revenue_per_product DESC").show()
        +--------------------+--------------------+-------------------------+           
        |          order_date|        product_name|daily_revenue_per_product|
        +--------------------+--------------------+-------------------------+
        |2013-07-25 00:00:...|Field & Stream Sp...|        5599.720153808594|
        |2013-07-25 00:00:...|Nike Men's Free 5...|        5099.490051269531|
        |2013-07-25 00:00:...|Diamondback Women...|        4499.700164794922|
        |2013-07-25 00:00:...|Perfect Fitness P...|       3359.4401054382324|
        |2013-07-25 00:00:...|Pelican Sunstream...|        2999.850082397461|
        |2013-07-25 00:00:...|O'Brien Men's Neo...|       2798.8799781799316|
        |2013-07-25 00:00:...|Nike Men's CJ Eli...|        1949.850082397461|
        |2013-07-25 00:00:...|Nike Men's Dri-FI...|                   1650.0|
        |2013-07-25 00:00:...|Under Armour Girl...|       1079.7300071716309|
        |2013-07-25 00:00:...|Bowflex SelectTec...|         599.989990234375|
        |2013-07-25 00:00:...|Elevation Trainin...|        319.9599914550781|
        |2013-07-25 00:00:...|Titleist Pro V1 H...|        207.9600067138672|
        |2013-07-25 00:00:...|Nike Men's Kobe I...|       199.99000549316406|
        |2013-07-25 00:00:...|Cleveland Golf Wo...|       119.98999786376953|
        |2013-07-25 00:00:...|TYR Boys' Team Di...|       119.97000122070312|
        |2013-07-25 00:00:...|Merrell Men's All...|       109.98999786376953|
        |2013-07-25 00:00:...|LIJA Women's Butt...|                    108.0|
        |2013-07-25 00:00:...|Nike Women's Lege...|                    100.0|
        |2013-07-25 00:00:...|Team Golf Tenness...|        99.95999908447266|
        |2013-07-25 00:00:...|Bridgestone e6 St...|        95.97000122070312|
        +--------------------+--------------------+-------------------------+
        only showing top 20 rows
```
**Tip:**
To reduce the number of tasks for this operation, we will execute the next statement before executing the sql query:
```python
sqlContext.setConf("spark.sql.shuffle.partitions", "2")
```


### SAVING DATAFRAME
1. We will use Hive to store the output. The name of the database will be carlos_sanchez_daily_revenue.
```python
>>> sqlContext.sql("CREATE DATABASE carlos_sanchez_daily_revenue");
DataFrame[result: string]
```
2. Save it into a HIVE table using ORC file format. For that aim, we must create the table.

```python
>>> sqlContext.sql("CREATE TABLE carlos_sanchez_daily_revenue.daily_revenue(order_date string, product_name string, daily_revenue_per_product float) stored as orc")
        DataFrame[result: string]
```

3. Select the info we want to insert into our new table:
```python
daily_revenue_per_product_df = sqlContext.sql("SELECT o.order_date, p.product_name, sum(oi.order_item_subtotal) daily_revenue_per_product \
        FROM orders o JOIN order_items oi \
        ON o.order_id = oi.order_item_order_id \
        JOIN products p \
        ON p.product_id = oi.order_item_product_id \
        WHERE o.order_status IN ('COMPLETE', 'CLOSED') \
        GROUP BY o.order_date, p.product_name \
        ORDER BY o.order_date, daily_revenue_per_product DESC")
```

4. We insert that info into the table:
```python    
daily_revenue_per_product_df.write.insertInto("carlos_sanchez_daily_revenue.daily_revenue")
```
5. We check the info from the table we have created
```python
>>> sqlContext.sql("select * from carlos_sanchez_daily_revenue.daily_revenue").show()
        +--------------------+--------------------+-------------------------+
        |          order_date|        product_name|daily_revenue_per_product|
        +--------------------+--------------------+-------------------------+
        |2013-07-25 00:00:...|Field & Stream Sp...|                  5599.72|
        |2013-07-25 00:00:...|Nike Men's Free 5...|                  5099.49|
        |2013-07-25 00:00:...|Diamondback Women...|                   4499.7|
        |2013-07-25 00:00:...|Perfect Fitness P...|                3359.4402|
        |2013-07-25 00:00:...|Pelican Sunstream...|                  2999.85|
        |2013-07-25 00:00:...|O'Brien Men's Neo...|                  2798.88|
        |2013-07-25 00:00:...|Nike Men's CJ Eli...|                1949.8501|
        |2013-07-25 00:00:...|Nike Men's Dri-FI...|                   1650.0|
        |2013-07-25 00:00:...|Under Armour Girl...|                  1079.73|
        |2013-07-25 00:00:...|Bowflex SelectTec...|                   599.99|
        |2013-07-25 00:00:...|Elevation Trainin...|                   319.96|
        |2013-07-25 00:00:...|Titleist Pro V1 H...|                   207.96|
        |2013-07-25 00:00:...|Nike Men's Kobe I...|                   199.99|
        |2013-07-25 00:00:...|Cleveland Golf Wo...|                   119.99|
        |2013-07-25 00:00:...|TYR Boys' Team Di...|                   119.97|
        |2013-07-25 00:00:...|Merrell Men's All...|                   109.99|
        |2013-07-25 00:00:...|LIJA Women's Butt...|                    108.0|
        |2013-07-25 00:00:...|Nike Women's Lege...|                    100.0|
        |2013-07-25 00:00:...|Team Golf Tenness...|                    99.96|
        |2013-07-25 00:00:...|Bridgestone e6 St...|                    95.97|
        +--------------------+--------------------+-------------------------+
```

**Tip:**
If we want help in order to know what are the functions available as for our dataframe, we will use:

```python
help(daily_revenue_per_product_df)
```



###DATAFRAME OPERATIONS
    Let us explore a few Dataframe operations:
        * show()
        * select
        * filter
        * join

To see the operations available in a dataframe, we can do:
```python
    help(daily_revenue_per_product_df)
```
To have help related with a certain function, we will use

```python
help(daily_revenue_per_product_df.write)
```

To show the schema of a dataframe:


we have two ways:
1.

```python
>>> daily_revenue_per_product_df.schema
StructType(List(StructField(order_date,StringType,true),StructField(product_name,StringType,true),StructField(daily_revenue_per_product,DoubleType,true)))
```
2.
```python  
>>> daily_revenue_per_product_df.printSchema()
root
|-- order_date: string (nullable = true)
|-- product_name: string (nullable = true)
|-- daily_revenue_per_product: double (nullable = true)
```

**Saving data**

1. Save data into a dataframe or table
```python
>>> daily_revenue_per_product_df.saveAsTable
```
2. Save data into a json file
```python
>>> daily_revenue_per_product_df.save("/user/carlos_sanchez/daily_revenue_save", "json")
```
or
```python
    >>> daily_revenue_per_product_df.write.json("/user/carlos_sanchez/daily_revenue_save")
```

Then, we can check whether the files have been saved:
```python
[carlos_sanchez@gw03 ~]$ hdfs dfs -ls /user/carlos_sanchez/daily_revenue_save
        Found 3 items
        -rw-r--r--   2 carlos_sanchez hdfs          0 2020-07-20 06:22 /user/carlos_sanchez/daily_revenue_save/_SUCCESS
        -rw-r--r--   2 carlos_sanchez hdfs     694532 2020-07-20 06:22 /user/carlos_sanchez/daily_revenue_save/part-r-00000-3addd6f4-93f1-473b-9c7b-0b5c7ef28952
        -rw-r--r--   2 carlos_sanchez hdfs     562032 2020-07-20 06:22 /user/carlos_sanchez/daily_revenue_save/part-r-00001-3addd6f4-93f1-473b-9c7b-0b5c7ef28952
```
And when we check the files created, we see they are in json format:
```python
[carlos_sanchez@gw03 ~]$ hdfs dfs -tail /user/carlos_sanchez/daily_revenue_save/part-r-00000-3addd6f4-93f1-473b-9c7b-0b5c7ef28952
        olf Dress","daily_revenue_per_product":108.0}
        {"order_date":"2014-02-08 00:00:00.0","product_name":"Bridgestone e6 Straight Distance NFL Tennesse","daily_revenue_per_product":63.97999954223633}
        {"order_date":"2014-02-08 00:00:00.0","product_name":"Titleist Pro V1 High Numbers Personalized Gol","daily_revenue_per_product":51.9900016784668}
        {"order_date":"2014-02-08 00:00:00.0","product_name":"Nike Dri-FIT Crew Sock 6 Pack","daily_revenue_per_product":44.0}
        {"order_date":"2014-02-08 00:00:00.0","product_name":"Under Armour Kids' Mercenary Slide","daily_revenue_per_product":27.989999771118164}
        {"order_date":"2014-02-09 00:00:00.0","product_name":"Field & Stream Sportsman 16 Gun Fire Safe","daily_revenue_per_product":9599.520263671875}
        {"order_date":"2014-02-09 00:00:00.0","product_name":"Diamondback Women's Serene Classic Comfort Bi","daily_revenue_per_product":7799.480285644531}
        {"order_date":"2014-02-09 00:00:00.0","product_name":"Perfect Fitness Perfect Rip Deck","daily_revenue_per_product":7798.70023727417}
```

**Filtering**
We have many ways:
1. With the select clause
```python

    >>> daily_revenue_per_product_df.select("order_date","daily_revenue_per_product").show()
    +--------------------+-------------------------+                                
    |          order_date|daily_revenue_per_product|
    +--------------------+-------------------------+
    |2013-07-25 00:00:...|        5599.720153808594|
    |2013-07-25 00:00:...|        5099.490051269531|
    |2013-07-25 00:00:...|        4499.700164794922|
    |2013-07-25 00:00:...|       3359.4401054382324|
    |2013-07-25 00:00:...|        2999.850082397461|
    |2013-07-25 00:00:...|       2798.8799781799316|
    |2013-07-25 00:00:...|        1949.850082397461|
    |2013-07-25 00:00:...|                   1650.0|
    |2013-07-25 00:00:...|       1079.7300071716309|
    |2013-07-25 00:00:...|         599.989990234375|
    |2013-07-25 00:00:...|        319.9599914550781|
    |2013-07-25 00:00:...|        207.9600067138672|
    |2013-07-25 00:00:...|       199.99000549316406|
    |2013-07-25 00:00:...|       119.98999786376953|
    |2013-07-25 00:00:...|       119.97000122070312|
    |2013-07-25 00:00:...|       109.98999786376953|
    |2013-07-25 00:00:...|                    108.0|
    |2013-07-25 00:00:...|                    100.0|
    |2013-07-25 00:00:...|        99.95999908447266|
    |2013-07-25 00:00:...|        95.97000122070312|
    +--------------------+-------------------------+
    only showing top 20 rows
```
2. With the filter clause:
```python
daily_revenue_per_product_df.filter(daily_revenue_per_product_df["order_date"== "2013-07-25 00:00":00.0])
    +--------------------+-------------------------+                                
    |          order_date|daily_revenue_per_product|
    +--------------------+-------------------------+
    |2013-07-25 00:00:...|        5599.720153808594|
    |2013-07-25 00:00:...|        5099.490051269531|
    |2013-07-25 00:00:...|        4499.700164794922|
    |2013-07-25 00:00:...|       3359.4401054382324|
    |2013-07-25 00:00:...|        2999.850082397461|
    |2013-07-25 00:00:...|       2798.8799781799316|
    |2013-07-25 00:00:...|        1949.850082397461|
    |2013-07-25 00:00:...|                   1650.0|
    |2013-07-25 00:00:...|       1079.7300071716309|
    |2013-07-25 00:00:...|         599.989990234375|
    |2013-07-25 00:00:...|        319.9599914550781|
    |2013-07-25 00:00:...|        207.9600067138672|
    |2013-07-25 00:00:...|       199.99000549316406|
    |2013-07-25 00:00:...|       119.98999786376953|
    |2013-07-25 00:00:...|       119.97000122070312|
    |2013-07-25 00:00:...|       109.98999786376953|
    |2013-07-25 00:00:...|                    108.0|
    |2013-07-25 00:00:...|                    100.0|
    |2013-07-25 00:00:...|        99.95999908447266|
    |2013-07-25 00:00:...|        95.97000122070312|
    +--------------------+-------------------------+
```

We can give aliases to the derived fields using alias function 
```python
orders.select(substring('order_date',1,8).alias('order_month')).show()
```
3. In sql way
```python
>>> orders.selectExpr('substring(order_date,1,7) as order_month')
    DataFrame[order_month: string]
```
4. Using "where" clause:

takes dataframes native style syntax
```python
orders.where(orders.order_status == 'COMPLETE').show()
+--------+--------------------+-----------------+------------+            
|order_id|          order_date|order_customer_id|order_status|
+--------+--------------------+-----------------+------------+
|       3|2013-07-25 00:00:...|            12111|    COMPLETE|
|       5|2013-07-25 00:00:...|            11318|    COMPLETE|
|       6|2013-07-25 00:00:...|             7130|    COMPLETE|
|       7|2013-07-25 00:00:...|             4530|    COMPLETE|
|      15|2013-07-25 00:00:...|             2568|    COMPLETE|
|      17|2013-07-25 00:00:...|             2667|    COMPLETE|
|      22|2013-07-25 00:00:...|              333|    COMPLETE|
|      26|2013-07-25 00:00:...|             7562|    COMPLETE|
|      28|2013-07-25 00:00:...|              656|    COMPLETE|
|      32|2013-07-25 00:00:...|             3960|    COMPLETE|
|      35|2013-07-25 00:00:...|             4840|    COMPLETE|
|      45|2013-07-25 00:00:...|             2636|    COMPLETE|
|      56|2013-07-25 00:00:...|            10519|    COMPLETE|
|      63|2013-07-25 00:00:...|             1148|    COMPLETE|
|      65|2013-07-25 00:00:...|             5903|    COMPLETE|
|      67|2013-07-25 00:00:...|             1406|    COMPLETE|
|      71|2013-07-25 00:00:...|             8646|    COMPLETE|
|      72|2013-07-25 00:00:...|             4349|    COMPLETE|
|      76|2013-07-25 00:00:...|             6898|    COMPLETE|
|      80|2013-07-25 00:00:...|             3007|    COMPLETE|
+--------+--------------------+-----------------+------------+
only showing top 20 rows
```
5. Using the "filter" clause:
```python
>>> orders.filter(orders.order_status.isin('COMPLETE', 'CLOSED')).show()
+--------+--------------------+-----------------+------------+
|order_id|          order_date|order_customer_id|order_status|
+--------+--------------------+-----------------+------------+
|       1|2013-07-25 00:00:...|            11599|      CLOSED|
|       3|2013-07-25 00:00:...|            12111|    COMPLETE|
|       4|2013-07-25 00:00:...|             8827|      CLOSED|
|       5|2013-07-25 00:00:...|            11318|    COMPLETE|
|       6|2013-07-25 00:00:...|             7130|    COMPLETE|
|       7|2013-07-25 00:00:...|             4530|    COMPLETE|
|      12|2013-07-25 00:00:...|             1837|      CLOSED|
|      15|2013-07-25 00:00:...|             2568|    COMPLETE|
|      17|2013-07-25 00:00:...|             2667|    COMPLETE|
|      18|2013-07-25 00:00:...|             1205|      CLOSED|
|      22|2013-07-25 00:00:...|              333|    COMPLETE|
|      24|2013-07-25 00:00:...|            11441|      CLOSED|
|      25|2013-07-25 00:00:...|             9503|      CLOSED|
|      26|2013-07-25 00:00:...|             7562|    COMPLETE|
|      28|2013-07-25 00:00:...|              656|    COMPLETE|
|      32|2013-07-25 00:00:...|             3960|    COMPLETE|
|      35|2013-07-25 00:00:...|             4840|    COMPLETE|
|      37|2013-07-25 00:00:...|             5863|      CLOSED|
|      45|2013-07-25 00:00:...|             2636|    COMPLETE|
|      51|2013-07-25 00:00:...|            12271|      CLOSED|
+--------+--------------------+-----------------+------------+
only showing top 20 rows

```
or, in SQL way syntax:
```python
>>> orders.filter("order_status in ('COMPLETE', 'CLOSED')").show()
+--------+--------------------+-----------------+------------+                  
|order_id|          order_date|order_customer_id|order_status|
+--------+--------------------+-----------------+------------+
|       1|2013-07-25 00:00:...|            11599|      CLOSED|
|       3|2013-07-25 00:00:...|            12111|    COMPLETE|
|       4|2013-07-25 00:00:...|             8827|      CLOSED|
|       5|2013-07-25 00:00:...|            11318|    COMPLETE|
|       6|2013-07-25 00:00:...|             7130|    COMPLETE|
|       7|2013-07-25 00:00:...|             4530|    COMPLETE|
|      12|2013-07-25 00:00:...|             1837|      CLOSED|
|      15|2013-07-25 00:00:...|             2568|    COMPLETE|
|      17|2013-07-25 00:00:...|             2667|    COMPLETE|
|      18|2013-07-25 00:00:...|             1205|      CLOSED|
|      22|2013-07-25 00:00:...|              333|    COMPLETE|
|      24|2013-07-25 00:00:...|            11441|      CLOSED|
|      25|2013-07-25 00:00:...|             9503|      CLOSED|
|      26|2013-07-25 00:00:...|             7562|    COMPLETE|
|      28|2013-07-25 00:00:...|              656|    COMPLETE|
|      32|2013-07-25 00:00:...|             3960|    COMPLETE|
|      35|2013-07-25 00:00:...|             4840|    COMPLETE|
|      37|2013-07-25 00:00:...|             5863|      CLOSED|
|      45|2013-07-25 00:00:...|             2636|    COMPLETE|
|      51|2013-07-25 00:00:...|            12271|      CLOSED|
+--------+--------------------+-----------------+------------+
only showing top 20 rows
```



**Launching pyspark**
we can do it in two ways:
1. Horton
export SPARK_MAJOR_VERSION=2 and then run spark-shell, pyspark, spark-sql
2. Cloudera
spark2-shell or pyspark2 or spark2-sql or spark2-submit


**Getting metadata about a file**

```python
[carlos_sanchez@gw03 ~]$ hadoop fsck /public/randomtextwriter/part-m-00000
DEPRECATED: Use of this script to execute hdfs command is deprecated.
Instead use the hdfs command for it
Connecting to namenode via http://172.16.1.101:50070/fsck?ugi=carlos_sanchez&path=%2Fpublic%2Frandomtextwriter%2Fpart-m-00000
FSCK started by carlos_sanchez (auth:SIMPLE) from /172.16.1.113 for path /public/randomtextwriter/part-m-00000 at Mon Jul 20 10:58:39 EDT 2020
.Status: HEALTHY
 Total size:    1102230331 B
 Total dirs:    0
 Total files:   1
 Total symlinks:        0
 Total blocks (validated):  9 (avg. block size 122470036 B) --------------> data is divided into 9 blocks (in 9 nodes in cluster))
 Minimally replicated blocks:   9 (100.0 %)
 Over-replicated blocks:    0 (0.0 %)
 Under-replicated blocks:   0 (0.0 %)
 Mis-replicated blocks:     0 (0.0 %)
 Default replication factor:    2
 Average block replication: 3.0
 Corrupt blocks:        0
 Missing replicas:      0 (0.0 %)
 Number of data-nodes:      5
 Number of racks:       1
FSCK ended at Mon Jul 20 10:58:39 EDT 2020 in 0 milliseconds
```

**Operations with dates**
Get all the orders which are placed on first of every month
We can get the min date with the min function:
```python
>>> from pyspark.sql.functions import min
>>> orders.select(min(orders.order_date)).show()
+-------------------          
|     min(order_date)|
+--------------------+
|2013-07-25 00:00:...|
+--------------------+
```
With this, we can get the year of a date:

```python
from pyspark.sql.functions import date_format
>>> orders.select(date_format(orders.order_date,'yyyy')).show()
+-----------------------------+
|date_format(order_date, yyyy)|
+-----------------------------+
|                         2013|
|                         2013|
|                         2013|
|                         2013|
|                         2013|
|                         2013|
|                         2013|
|                         2013|
|                         2013|
|                         2013|
|                         2013|
|                         2013|
|                         2013|
|                         2013|
|                         2013|
|                         2013|
|                         2013|
|                         2013|
|                         2013|
|                         2013|
+-----------------------------+
only showing top 20 rows
```

To get the month we dan do:
```python
>>> orders.select(date_format(orders.order_date,'MM')).show()
+---------------------------+
|date_format(order_date, MM)|
+---------------------------+
|                         07|
|                         07|
|                         07|
|                         07|
|                         07|
|                         07|
|                         07|
|                         07|
|                         07|
|                         07|
|                         07|
|                         07|
|                         07|
|                         07|
|                         07|
|                         07|
|                         07|
|                         07|
|                         07|
|                         07|
+--------------------------
```
To get the day of month we dan do:
```python
>>> orders.select(date_format(orders.order_date,'dd')).show()
+---------------------------+
|date_format(order_date, dd)|
+---------------------------+
|                         25|
|                         25|
|                         25|
|                         25|
|                         25|
|                         25|
|                         25|
|                         25|
|                         25|
|                         25|
|                         25|
|                         25|
|                         25|
|                         25|
|                         25|
|                         25|
|                         25|
|                         25|
|                         25|
|                         25|
+---------------------------+
only showing top 20 rows
```
To know the day within the year:
```python
>>> orders.select(date_format(orders.order_date,'DD')).show()
+---------------------------+
|date_format(order_date, DD)|
+---------------------------+
|                        206|
|                        206|
|                        206|
|                        206|
|                        206|
|                        206|
|                        206|
|                        206|
|                        206|
|                        206|
|                        206|
|                        206|
|                        206|
|                        206|
|                        206|
|                        206|
|                        206|
|                        206|
|                        206|
|                        206|
+---------------------------+
only showing top 20 rows
```

To get the orders in the first day of every month
```python
            >>> orders.filter(date_format(orders.order_date, 'dd')=='01').show()
            +--------+--------------------+-----------------+---------------+
            |order_id|          order_date|order_customer_id|   order_status|
            +--------+--------------------+-----------------+---------------+
            |    1297|2013-08-01 00:00:...|            11607|       COMPLETE|
            |    1298|2013-08-01 00:00:...|             5105|         CLOSED|
            |    1299|2013-08-01 00:00:...|             7802|       COMPLETE|
            |    1300|2013-08-01 00:00:...|              553|PENDING_PAYMENT|
            |    1301|2013-08-01 00:00:...|             1604|PENDING_PAYMENT|
            |    1302|2013-08-01 00:00:...|             1695|       COMPLETE|
            |    1303|2013-08-01 00:00:...|             7018|     PROCESSING|
            |    1304|2013-08-01 00:00:...|             2059|       COMPLETE|
            |    1305|2013-08-01 00:00:...|             3844|       COMPLETE|
            |    1306|2013-08-01 00:00:...|            11672|PENDING_PAYMENT|
            |    1307|2013-08-01 00:00:...|             4474|       COMPLETE|
            |    1308|2013-08-01 00:00:...|            11645|        PENDING|
            |    1309|2013-08-01 00:00:...|             2367|         CLOSED|
            |    1310|2013-08-01 00:00:...|             5602|        PENDING|
            |    1311|2013-08-01 00:00:...|             5396|PENDING_PAYMENT|
            |    1312|2013-08-01 00:00:...|            12291|       COMPLETE|
            |    1313|2013-08-01 00:00:...|             3471|       CANCELED|
            |    1314|2013-08-01 00:00:...|            10993|       COMPLETE|
            |    1315|2013-08-01 00:00:...|             5660|       COMPLETE|
            |    1316|2013-08-01 00:00:...|             6376|PENDING_PAYMENT|
            +--------+--------------------+-----------------+---------------+
            only showing top 20 rows
```
or we can do it with sql stype syntax:
```python
            >>> orders.filter("date_format(order_date,'dd')='01'").show()

            +--------+--------------------+-----------------+---------------+
            |order_id|          order_date|order_customer_id|   order_status|
            +--------+--------------------+-----------------+---------------+
            |    1297|2013-08-01 00:00:...|            11607|       COMPLETE|
            |    1298|2013-08-01 00:00:...|             5105|         CLOSED|
            |    1299|2013-08-01 00:00:...|             7802|       COMPLETE|
            |    1300|2013-08-01 00:00:...|              553|PENDING_PAYMENT|
            |    1301|2013-08-01 00:00:...|             1604|PENDING_PAYMENT|
            |    1302|2013-08-01 00:00:...|             1695|       COMPLETE|
            |    1303|2013-08-01 00:00:...|             7018|     PROCESSING|
            |    1304|2013-08-01 00:00:...|             2059|       COMPLETE|
            |    1305|2013-08-01 00:00:...|             3844|       COMPLETE|
            |    1306|2013-08-01 00:00:...|            11672|PENDING_PAYMENT|
            |    1307|2013-08-01 00:00:...|             4474|       COMPLETE|
            |    1308|2013-08-01 00:00:...|            11645|        PENDING|
            |    1309|2013-08-01 00:00:...|             2367|         CLOSED|
            |    1310|2013-08-01 00:00:...|             5602|        PENDING|
            |    1311|2013-08-01 00:00:...|             5396|PENDING_PAYMENT|
            |    1312|2013-08-01 00:00:...|            12291|       COMPLETE|
            |    1313|2013-08-01 00:00:...|             3471|       CANCELED|
            |    1314|2013-08-01 00:00:...|            10993|       COMPLETE|
            |    1315|2013-08-01 00:00:...|             5660|       COMPLETE|
            |    1316|2013-08-01 00:00:...|             6376|PENDING_PAYMENT|
            +--------+--------------------+-----------------+---------------+
            only showing top 20 rows
```

**Create dataframe from csv file**
```python
>>> ordersDF = spark.read.csv('/public/retail_db/orders')
>>> type(ordersDF)
<class 'pyspark.sql.dataframe.DataFrame'>


>>> ordersDF.first()
Row(_c0=u'1', _c1=u'2013-07-25 00:00:00.0', _c2=u'11599', _c3=u'CLOSED')
>>> 


As we can see, rows do not have names to refer to them. For that reason we can name columns and rows (by default, we see that all columns are given "c0", "c1"...)
>>> ordersDF.select('_c0', '_c1').show()                                        
[Stage 1:>                                                          (0 + 1) / 1]
+---+--------------------+                                                      
|_c0|                 _c1|
+---+--------------------+
|  1|2013-07-25 00:00:...|
|  2|2013-07-25 00:00:...|
|  3|2013-07-25 00:00:...|
|  4|2013-07-25 00:00:...|
|  5|2013-07-25 00:00:...|
|  6|2013-07-25 00:00:...|
|  7|2013-07-25 00:00:...|
|  8|2013-07-25 00:00:...|
|  9|2013-07-25 00:00:...|
| 10|2013-07-25 00:00:...|
| 11|2013-07-25 00:00:...|
| 12|2013-07-25 00:00:...|
| 13|2013-07-25 00:00:...|
| 14|2013-07-25 00:00:...|
| 15|2013-07-25 00:00:...|
| 16|2013-07-25 00:00:...|
| 17|2013-07-25 00:00:...|
| 18|2013-07-25 00:00:...|
| 19|2013-07-25 00:00:...|
| 20|2013-07-25 00:00:...|
+---+--------------------+
only showing top 20 rows


```

**Showing dataframe sample**

This statement will show the complete information about the table.
```python

            >>> ordersDF.show(10, False)
            +---+---------------------+-----+---------------+            |_c0_c1                  |_c2  |_c3            |
            +---+---------------------+-----+---------------+
            |1  |2013-07-25 00:00:00.0|11599|CLOSED         |
            |2  |2013-07-25 00:00:00.0|256  |PENDING_PAYMENT|
            |3  |2013-07-25 00:00:00.0|12111|COMPLETE       |
            |4  |2013-07-25 00:00:00.0|8827 |CLOSED         |
            |5  |2013-07-25 00:00:00.0|11318|COMPLETE       |
            |6  |2013-07-25 00:00:00.0|7130 |COMPLETE       |
            |7  |2013-07-25 00:00:00.0|4530 |COMPLETE       |
            |8  |2013-07-25 00:00:00.0|2911 |PROCESSING     |
            |9  |2013-07-25 00:00:00.0|5657 |PENDING_PAYMENT|
            |10 |2013-07-25 00:00:00.0|5648 |PENDING_PAYMENT|
            +---+---------------------+-----+---------------+
```


**Showing schema**

```python
ordersDF.printSchema()
root
    |-- order_customer_id: long (nullable = true)
    |-- order_date: string (nullable = true)
    |-- order_id: long (nullable = true)
    |-- order_status: string (nullable = true)
```

**Showing data type**

```python
>>> ordersDF.describe()
            DataFrame[summary: string, _c0: string, _c1: string, _c2: string, _c3: string] 


            - We will be able to see some statistical information about our table
            >>> ordersDF.describe().show()
            +-------+------------------+--------------------+-----------------+---------------+
            |summary|               _c0|                 _c1|              _c2|            _c3|
            +-------+------------------+--------------------+-----------------+---------------+
            |  count|             68883|               68883|            68883|          68883|
            |   mean|           34442.0|                null|6216.571098819738|           null|
            | stddev|19884.953633337947|                null|3586.205241263963|           null|
            |    min|                 1|2013-07-25 00:00:...|                1|       CANCELED|
            |    max|              9999|2014-07-24 00:00:...|             9999|SUSPECTED_FRAUD|
            +-------+------------------+--------------------+-----------------+---------------+>>> ordersDF.describe()
            DataFrame[summary: string, _c0: string, _c1: string, _c2: string, _c3: string] 
```

We will be able to see some statistical information about our table

```python
            >>> ordersDF.describe().show()
            +-------+------------------+--------------------+-----------------+---------------+
            |summary|               _c0|                 _c1|              _c2|            _c3|
            +-------+------------------+--------------------+-----------------+---------------+
            |  count|             68883|               68883|            68883|          68883|
            |   mean|           34442.0|                null|6216.571098819738|           null|
            | stddev|19884.953633337947|                null|3586.205241263963|           null|
            |    min|                 1|2013-07-25 00:00:...|                1|       CANCELED|
            |    max|              9999|2014-07-24 00:00:...|             9999|SUSPECTED_FRAUD|
            +-------+------------------+--------------------+-----------------+---------------+
```

**Get the number of elements**
count


**Collect**
To convert dataframe into an array (or python collection)


Once the dataframe is created, we can process data using 2 approaches:
1. Native dataframe APIs
```python
        >>> ordersDF = spark.read.json('/public/retail_db_json/orders')
```
We can print the schema to understand the schema related details:

```python
        >>> ordersDF.printSchema()
        root
         |-- order_customer_id: long (nullable = true)
         |-- order_date: string (nullable = true)
         |-- order_id: long (nullable = true)
         |-- order_status: string (nullable = true)
```

Now, if I want to select data from the dataframe:
```python
        >>> ordersDF.select('order_id', 'order_date').show()
        +--------+--------------------+                                                 
        |order_id|          order_date|
        +--------+--------------------+
        |       1|2013-07-25 00:00:...|
        |       2|2013-07-25 00:00:...|
        |       3|2013-07-25 00:00:...|
        |       4|2013-07-25 00:00:...|
        |       5|2013-07-25 00:00:...|
        |       6|2013-07-25 00:00:...|
        |       7|2013-07-25 00:00:...|
        |       8|2013-07-25 00:00:...|
        |       9|2013-07-25 00:00:...|
        |      10|2013-07-25 00:00:...|
        |      11|2013-07-25 00:00:...|
        |      12|2013-07-25 00:00:...|
        |      13|2013-07-25 00:00:...|
        |      14|2013-07-25 00:00:...|
        |      15|2013-07-25 00:00:...|
        |      16|2013-07-25 00:00:...|
        |      17|2013-07-25 00:00:...|
        |      18|2013-07-25 00:00:...|
        |      19|2013-07-25 00:00:...|
        |      20|2013-07-25 00:00:...|
        +--------+--------------------+
        only showing top 20 rows
```

2. Register as temp table and run queries using spark.sql

```python
        ordersDf.createTempView('orders')
        >>> spark.sql('select * from orders').show()

        +-----------------+--------------------+--------+---------------+               
        |order_customer_id|          order_date|order_id|   order_status|
        +-----------------+--------------------+--------+---------------+
        |            11599|2013-07-25 00:00:...|       1|         CLOSED|
        |              256|2013-07-25 00:00:...|       2|PENDING_PAYMENT|
        |            12111|2013-07-25 00:00:...|       3|       COMPLETE|
        |             8827|2013-07-25 00:00:...|       4|         CLOSED|
        |            11318|2013-07-25 00:00:...|       5|       COMPLETE|
        |             7130|2013-07-25 00:00:...|       6|       COMPLETE|
        |             4530|2013-07-25 00:00:...|       7|       COMPLETE|
        |             2911|2013-07-25 00:00:...|       8|     PROCESSING|
        |             5657|2013-07-25 00:00:...|       9|PENDING_PAYMENT|
        |             5648|2013-07-25 00:00:...|      10|PENDING_PAYMENT|
        |              918|2013-07-25 00:00:...|      11| PAYMENT_REVIEW|
        |             1837|2013-07-25 00:00:...|      12|         CLOSED|
        |             9149|2013-07-25 00:00:...|      13|PENDING_PAYMENT|
        |             9842|2013-07-25 00:00:...|      14|     PROCESSING|
        |             2568|2013-07-25 00:00:...|      15|       COMPLETE|
        |             7276|2013-07-25 00:00:...|      16|PENDING_PAYMENT|
        |             2667|2013-07-25 00:00:...|      17|       COMPLETE|
        |             1205|2013-07-25 00:00:...|      18|         CLOSED|
        |             9488|2013-07-25 00:00:...|      19|PENDING_PAYMENT|
        |             9198|2013-07-25 00:00:...|      20|     PROCESSING|
        +-----------------+--------------------+--------+---------------+
only showing top 20 rows
```
To work with Data frames as well as Spark SQL, we need to create an object of type SparkSession.

When launching spark with pyspark, sc (SparkContext) and spark variables are created for us.
If we wanted to create the spark session ourselves:
1. In the console:
```python
    python3 install -m pyspark ---> in the console 
    python3
```

2. Programatically:
```python
    import pyspark
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('Create a Spark Session Demo').master('local').getOrCreate()
```



##Create dataframes from text files
We can use spark.read.csv or spark.read.text to read text data.
spark.read.csv can be used for comma separated data. Default field names will be in the form of _c0, _c1, etc

1. Inferring data types from content.
```python
            ordersDF = spark.read.csv('/public/retail_db/orders')
            >>> ordersDF.printSchema()
            root
             |-- order_customer_id: long (nullable = true)
             |-- order_date: string (nullable = true)
             |-- order_id: long (nullable = true)
             |-- order_status: string (nullable = true)
```
As we can see, data types are not inferred. To do so, we can have another way to read the file while designing the schema:

2. Defining the schema  and data types
```python
             ordersDF = spark.read.csv('/public/retail_db/orders', sep=',', schema = 'order_id int, order_date string, order_customer_id int, order_status string')
             >>> ordersDF.printSchema()
            root
             |-- order_id: integer (nullable = true)
             |-- order_date: string (nullable = true)
             |-- order_customer_id: integer (nullable = true)
             |-- order_status: string (nullable = true)
```
or 

```python
ordersDF = read.format('csv').option('sep', ',').schema('order_id int, order_date string, order_customer_id int, order_status string'). \
                 load('/public/retail_db/orders')
```
or 
**In newer versions** (we can define the data type of the fields while loading the data)
```python
orders = spark.read. \
      format('csv'). \
      schema('order_id int, order_date string, order_customer_id int, order_status string'). \
      load('/public/retail_db/orders')
```

**In older versions** (we have to load and define the data types of the fields in two steps)
a) Read csv file and convert it to a dataframe:
```python
orderItemsCSV = spark.read. \
    csv('/public/retail_db/order_items'). \
    toDF('order_item_id', 'order_item_order_id', 'order_item_product_id', 
    'order_item_quantity', 'order_item_subtotal', 'order_item_product_price')
```
b) Define data types: 
```python            
from pyspark.sql.types import IntegerType, FloatType
orders = ordersCSV. \
withColumn('order_id', ordersCSV.order_id.cast(IntegerType())). \
withColumn('order_customer_id', ordersCSV.order_customer_id.cast(IntegerType()))

```

3. Typecasting the values)    :
```python
ordersDF.select(orders.order_id.cast("int"))
```
or
```python
ordersDF.select(orders.order_id.cast("int"), orders.order_date, orders.order_customer_id.cast(IntegerType()),orders.order_status )
```
or
```python
orders.withColumn('order_id', orders.order_id.cast("int")).\
      withColumn('order_customer_id', orders.order_customer_id.cast(IntegerType()))
```

4. To read fixed length data where there is no delimiter
    spark.read.text can be used to read fixed length data where there is no delimiter. Default field name is value.
```python
orders= spark.read.text('/public/retail_db/orders')
>>> orders.printSchema()
root
|-- value: string (nullable = true)
```
As we can see, it consider the data as a string type:

```python
            >>> orders.show()
            +--------------------+                                                          
            |               value|
            +--------------------+
            |1,2013-07-25 00:0...|
            |2,2013-07-25 00:0...|
            |3,2013-07-25 00:0...|
            |4,2013-07-25 00:0...|
            |5,2013-07-25 00:0...|
            |6,2013-07-25 00:0...|
            |7,2013-07-25 00:0...|
            |8,2013-07-25 00:0...|
            |9,2013-07-25 00:0...|
            |10,2013-07-25 00:...|
            |11,2013-07-25 00:...|
            |12,2013-07-25 00:...|
            |13,2013-07-25 00:...|
            |14,2013-07-25 00:...|
            |15,2013-07-25 00:...|
            |16,2013-07-25 00:...|
            |17,2013-07-25 00:...|
            |18,2013-07-25 00:...|
            |19,2013-07-25 00:...|
            |20,2013-07-25 00:...|
            +--------------------+
            only showing top 20 rows
```
As we can see, the view is truncated. If we wanted the table not to be show in truncated way, what we can do is:

```python
            >>> orders.show(truncate=False)
            +---------------------------------------------+
            |value                                        |
            +---------------------------------------------+
            |1,2013-07-25 00:00:00.0,11599,CLOSED         |
            |2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT  |
            |3,2013-07-25 00:00:00.0,12111,COMPLETE       |
            |4,2013-07-25 00:00:00.0,8827,CLOSED          |
            |5,2013-07-25 00:00:00.0,11318,COMPLETE       |
            |6,2013-07-25 00:00:00.0,7130,COMPLETE        |
            |7,2013-07-25 00:00:00.0,4530,COMPLETE        |
            |8,2013-07-25 00:00:00.0,2911,PROCESSING      |
            |9,2013-07-25 00:00:00.0,5657,PENDING_PAYMENT |
            |10,2013-07-25 00:00:00.0,5648,PENDING_PAYMENT|
            |11,2013-07-25 00:00:00.0,918,PAYMENT_REVIEW  |
            |12,2013-07-25 00:00:00.0,1837,CLOSED         |
            |13,2013-07-25 00:00:00.0,9149,PENDING_PAYMENT|
            |14,2013-07-25 00:00:00.0,9842,PROCESSING     |
            |15,2013-07-25 00:00:00.0,2568,COMPLETE       |
            |16,2013-07-25 00:00:00.0,7276,PENDING_PAYMENT|
            |17,2013-07-25 00:00:00.0,2667,COMPLETE       |
            |18,2013-07-25 00:00:00.0,1205,CLOSED         |
            |19,2013-07-25 00:00:00.0,9488,PENDING_PAYMENT|
            |20,2013-07-25 00:00:00.0,9198,PROCESSING     |
            +---------------------------------------------+
            only showing top 20 rows
```


- We can define attribute names using toDF function
- In either of the case data will be represented as strings
- We can convert data types by using cast function 
- 
```python
df.select(df.field.cast(IntegerType()))
```

Creating dataframes from hive tables
--------------------------------------
If Hive and Spark are integrated, we can create data frames from data in Hive tables or run Spark SQL queries against it.
    We launch pyspark in the latest version
    pyspark2 --master yarn --conf spark.ui.port=0
    hive ---> to execute hive
    - We can use spark.read.table to read data from Hive tables into Data Frame
    orders = spark.read.table('carlos_sanchez_retail_db_txt.orders')
    >>> orders.printSchema()
    root
     |-- order_id: integer (nullable = true)
     |-- order_date: string (nullable = true)
     |-- order_customer_id: integer (nullable = true)
     |-- order_status: string (nullable = true)


    >>> orders.show()
    +--------+--------------------+-----------------+---------------+               
    |order_id|          order_date|order_customer_id|   order_status|
    +--------+--------------------+-----------------+---------------+
    |       1|2013-07-25 00:00:...|            11599|         CLOSED|
    |       2|2013-07-25 00:00:...|              256|PENDING_PAYMENT|
    |       3|2013-07-25 00:00:...|            12111|       COMPLETE|
    |       4|2013-07-25 00:00:...|             8827|         CLOSED|
    |       5|2013-07-25 00:00:...|            11318|       COMPLETE|
    |       6|2013-07-25 00:00:...|             7130|       COMPLETE|
    |       7|2013-07-25 00:00:...|             4530|       COMPLETE|
    |       8|2013-07-25 00:00:...|             2911|     PROCESSING|
    |       9|2013-07-25 00:00:...|             5657|PENDING_PAYMENT|
    |      10|2013-07-25 00:00:...|             5648|PENDING_PAYMENT|
    |      11|2013-07-25 00:00:...|              918| PAYMENT_REVIEW|
    |      12|2013-07-25 00:00:...|             1837|         CLOSED|
    |      13|2013-07-25 00:00:...|             9149|PENDING_PAYMENT|
    |      14|2013-07-25 00:00:...|             9842|     PROCESSING|
    |      15|2013-07-25 00:00:...|             2568|       COMPLETE|
    |      16|2013-07-25 00:00:...|             7276|PENDING_PAYMENT|
    |      17|2013-07-25 00:00:...|             2667|       COMPLETE|
    |      18|2013-07-25 00:00:...|             1205|         CLOSED|
    |      19|2013-07-25 00:00:...|             9488|PENDING_PAYMENT|
    |      20|2013-07-25 00:00:...|             9198|     PROCESSING|
    +--------+--------------------+-----------------+---------------+
    only showing top 20 rows

    - We can prefix database name to table name while reading Hive tables into Data Frame
    - We can also run Hive queries directly using spark.sql
    spark.sql('select * from carlos_sanchez_retail_db_txt.orders')

    - Both spark.read.table and spark.sql returns Data Frame


## Creating dataframes from hive tables

- In Pycharm, we need to copy relevant jdbc jar file to SPARK_HOME/jars
- We can either use spark.read.format(‘jdbc’) with options or spark.read.jdbc with jdbc url, table name and other properties as dict to read data from remote relational databases.
1. We need to make sure jdbc jar file is registered using --packages or --jars and --driver-class-path while launching pyspark
Example:
We search the jar file (it is in this location)
```python
cd /usr/share/java
[carlos_sanchez@gw03 java]$ ls -ltr mysql-connector-java.jar 
-rw-r--r-- 1 root root 883898 Jun  9  2014 mysql-connector-java.jar
```
Now we know how to launch pyspark with the mysql jar:

```python
pyspark2 \
    --master yarn \
    --conf spark.ui.port=0 \
    --jars /usr/share/java/mysql-connector-java.jar \
    --driver-class-path /usr/share/java/mysql-connector-java.jar
```
If we wanted to search for information about jdbc method, we will use:

```python
help(spark.read.jdbc)
```

2. We connect to the database in order to check whether we can use the connection to our spark session:
```python
[carlos_sanchez@gw03 ~]$ mysql -h ms.itversity.com -u retail_user -p
                Enter password: 

                Welcome to the MySQL monitor.  Commands end with ; or \g.
                Your MySQL connection id is 4148167
                Server version: 5.5.60-MariaDB MariaDB Server

                Copyright (c) 2000, 2019, Oracle and/or its affiliates. All rights reserved.

                Oracle is a registered trademark of Oracle Corporation and/or its
                affiliates. Other names may be trademarks of their respective
                owners.

                Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

                mysql> 
                mysql> 

                mysql> use retail_db;
                Reading table information for completion of table and column names
                You can turn off this feature to get a quicker startup with -A

                Database changed
                mysql> show tables;
                +---------------------+
                | Tables_in_retail_db |
                +---------------------+
                | categories          |
                | customers           |
                | departments         |
                | order_items         |
                | order_items_nopk    |
                | orders              |
                | products            |
                +---------------------+
                7 rows in set (0.00 sec)
```

Now we can read the table (using the same credentials as the above connection):
```python                
orders = spark.read. \
         format('jdbc'). \
         option('url', 'jdbc:mysql://ms.itversity.com'). \
         option('dbtable', 'retail_db.orders'). \
         option('user', 'retail_user'). \
         option('password', 'itversity'). \
         load()
```
Another way:
```
                 orderItems = spark.read. \
                    jdbc("jdbc:mysql://ms.itversity.com", "retail_db.order_items",
                    properties={"user": "retail_user",
                    "password": "itversity",
                    "numPartitions": "4",
                    "partitionColumn": "order_item_order_id",
                    "lowerBound": "10000",
                    "upperBound": "20000"})
```

Traditionally, we use **Sqoop** to ingest data from relational database or jdbc into hdfs, and process the data further using Hive or spark.sql. We can move some of the sqoop applications into spark using additional parameters

Spark facilitates the way to import data from other origins: While reading data, we can define number of partitions (using numPartitions), criteria to divide data into partitions (partitionColumn, lowerBound, upperBound)
    * To define the number of threads we can use the numPartitions
    * To define the number of columns we can use the partitionColumn (Partitioning can be done only on numeric fields)
    * To define boundary partition we have lowerbound and upperbound

If lowerBound and upperBound is specified, it will generate strides depending up on number of partitions and then process entire data. Here is the example:
    - We are trying to read order_items data with 4 as numPartitions
    - partitionColumn – order_item_order_id
    - lowerBound – 10000
    - upperBound – 20000
    - order_item_order_id is in the range of 1 and 68883
    - But as we define lowerBound as 10000 and upperBound as 20000, here will be strides – 1 to 12499, 12500 to 14999, 15000 to 17499, 17500 to maximum of order_item_order_id
    - You can check the data in the output path mentioned



###SPARK SQL
It will display the list of the functions that are available
```python
from pyspark.sql import functions
help(functions)
```
or
```python
    spark.sql('describe function substring').show(truncate=False)
```
We can also use Spark SQL to process data in data frames.
    - We can get list of tables by using spark.sql('show tables')
    - We can register data frame as temporary view df.createTempView("view_name")
    - Output of show tables show the temporary tables as well
    - Once temp view is created, we can use SQL style syntax and run queries against the tables/views
    - Most of the hive queries will work out of the box

Main package for functions: pyspark.sql.functions.
We can import by saying from pyspark.sql import functions as sf
- Functions in pyspark.sql are similar to the functions in traditional databases. These can be categorized into:
    - String manipulation
    - Date manipulation
    - Type casting
    - Expressions such as case when
        - We will see some of the functions in action
            - substring
            - lower, upper
            - transient_lastDdlTime
            - date_format
            - trunc
            - Type Casting
            - case when

### JOINING DATASETS
Quite often we need to deal with multiple datasets which are related to each other.

- We need to first understand the relationship with respect to datasets
- All our datasets have relationships defined between them
    - order and order_items are transaction tables. order is parent and order_items is child. The relationship is established between the two using order_id (in order_items, it is represented as order_item_order_id)
    - We also have product catalogue normalized into 3 tables - products, categories and departments (with relationships established in that order)
    - We also have a customer table
    - There is a relationship between customers and orders - customers is parent dataset as one customer can place multiple orders.
    - There is a relationship between the product catalog and order_items via products - products is parent dataset as one product cam be ordered as part of multiple order_items
- Determine the type of join - inner or outer (left or right or full)
- Dataframes have an API called join to perform joins
- We can make the outer join by passing an additional argument
- Examples:

**Example 1**
        
Get all the order items corresponding to COMPLETE OR CLOSED orders

1. we load data
```python
orders = spark.read. \
... format('csv'). \
... schema('order_id int, order_date string, order_customer_id int, order_status string'). \
... load('/public/retail_db/orders')


orderItemsCSV = spark.read.csv('/public/retail_db/order_items'). \
    ... toDF('order_item_id', 'order_item_order_id', 'order_item_product_id','order_item_quantity', 'order_item_subtotal', 'order_item_product_price')
```
2. We define data types
```python
from pyspark.sql.types import IntegerType, FloatType
orderItems = orderItemsCSV.\
... withColumn('order_item_id', orderItemsCSV.order_item_id.cast(IntegerType())). \
...withColumn('order_item_order_id', orderItemsCSV.order_item_order_id.cast(IntegerType())). \
...withColumn('order_item_product_id', orderItemsCSV.order_item_product_id.cast(IntegerType())). \
...withColumn('order_item_quantity', orderItemsCSV.order_item_quantity.cast(IntegerType())). \
...withColumn('order_item_subtotal', orderItemsCSV.order_item_subtotal.cast(FloatType())). \
...withColumn('order_item_product_price', orderItemsCSV.order_item_product_price.cast(FloatType()))


>>> orders.show()
+--------+--------------------+-----------------+---------------+
|order_id|          order_date|order_customer_id|   order_status|
+--------+--------------------+-----------------+---------------+
|       1|2013-07-25 00:00:...|            11599|         CLOSED|
|       2|2013-07-25 00:00:...|              256|PENDING_PAYMENT|
|       3|2013-07-25 00:00:...|            12111|       COMPLETE|
|       4|2013-07-25 00:00:...|             8827|         CLOSED|
|       5|2013-07-25 00:00:...|            11318|       COMPLETE|
|       6|2013-07-25 00:00:...|             7130|       COMPLETE|
|       7|2013-07-25 00:00:...|             4530|       COMPLETE|
|       8|2013-07-25 00:00:...|             2911|     PROCESSING|
|       9|2013-07-25 00:00:...|             5657|PENDING_PAYMENT|
|      10|2013-07-25 00:00:...|             5648|PENDING_PAYMENT|
|      11|2013-07-25 00:00:...|              918| PAYMENT_REVIEW|
|      12|2013-07-25 00:00:...|             1837|         CLOSED|
|      13|2013-07-25 00:00:...|             9149|PENDING_PAYMENT|
|      14|2013-07-25 00:00:...|             9842|     PROCESSING|
|      15|2013-07-25 00:00:...|             2568|       COMPLETE|
|      16|2013-07-25 00:00:...|             7276|PENDING_PAYMENT|
|      17|2013-07-25 00:00:...|             2667|       COMPLETE|
|      18|2013-07-25 00:00:...|             1205|         CLOSED|
|      19|2013-07-25 00:00:...|             9488|PENDING_PAYMENT|
|      20|2013-07-25 00:00:...|             9198|     PROCESSING|
+--------+--------------------+-----------------+---------------+
only showing top 20 rows
```
3. What we want is to display filter the COMPLETE or CLOSED orders
There are two possible solutions:
- Filter the COMPLETE or CLOSED orders and then join with order_items (this is more efficient as susequent steps operate with less data)
- Join with order_items and then filter the COMPLETE or CLOSED orders

Let's get the first option (filterig first ):

**Dataframe style**
```python
ordersFiltered = orders.where(orders.order_status.isin('COMPLETE', 'CLOSED'))
```

**SQL style:**
```python        
ordersFiltered = orders.where("order_status in ('COMPLETE', 'CLOSED')")
```
4. we execute the join in "dataframe style" ("inner" can be ommited because it will execute a inner join by default) :

```python
>>> ordersJoin = ordersFiltered.join(orderItems, ordersFiltered.order_id == orderItems.order_item_order_id, 'inner')
>>> type(ordersJoin)
<class 'pyspark.sql.dataframe.DataFrame'>
```
5. Get all the orders where there are no corresponding order_items  
    order is the parent table and order_items. We shouldn't have orders in COMPLETE or CLOSED state not having a corresponding order_item.

As we can see, we have 68883 distinct orders: 
```python
>>> orders.select('order_id').distinct().count()
68883 
>>> orderItems.select('order_item_order_id').distinct().count()
57431
```
It means there are 68883 - 57431 orders that do not have any corresponding order_item.To check it we will perform a left outer join:

```python
ordersLefOuterJoin = orders. \
    join(orderItems,
         orders.order_id == orderItems.order_item_order_id,
         'left' ---> we could have said 'outer', because outer has left as default
>>> ordersLeftOuterJoin.printSchema()
root
 |-- order_id: integer (nullable = true)
 |-- order_date: string (nullable = true)
 |-- order_customer_id: integer (nullable = true)
 |-- order_status: string (nullable = true)
 |-- order_item_id: integer (nullable = true)
 |-- order_item_order_id: integer (nullable = true)
 |-- order_item_product_id: integer (nullable = true)
 |-- order_item_quantity: integer (nullable = true)
 |-- order_item_subtotal: float (nullable = true)
 |-- order_item_product_price: float (nullable = true)


 >>> ordersLeftOuterJoin.show()
>>> ordersLeftOuterJoin.show()
+--------+--------------------+-----------------+---------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|order_id|order_date|order_customer_id|order_status|order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|+--------+--------------------+-----------------+---------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|       1|2013-07-25 00:00:...|11599|         CLOSED|   1|      1|      957|      1| 299.98|      299.98|
|       2|2013-07-25 00:00:...|  256|PENDING_PAYMENT|   4|      2|      403|      1| 129.99|      129.99|
|       2|2013-07-25 00:00:...|  256|PENDING_PAYMENT|   3|      2|      502|      5|  250.0|        50.0|
|       2|2013-07-25 00:00:...|  256|PENDING_PAYMENT|   2|      2|     1073|      1| 199.99|      199.99|
|       3|2013-07-25 00:00:...|12111|       COMPLETE|null|   null|     null|   null|   null|        null|
|       4|2013-07-25 00:00:...| 8827|         CLOSED|   8|      4|     1014|      4| 199.92|       49.98|
|       4|2013-07-25 00:00:...| 8827|         CLOSED|   7|      4|      502|      3|  150.0|        50.0|
|       4|2013-07-25 00:00:...| 8827|         CLOSED|   6|      4|      365|      5| 299.95|       59.99|
|       4|2013-07-25 00:00:...| 8827|         CLOSED|   5|      4|      897|      2|  49.98|       24.99|
|       5|2013-07-25 00:00:...|11318|       COMPLETE|  13|      5|      403|      1| 129.99|      129.99|
|       5|2013-07-25 00:00:...|11318|       COMPLETE|  12|      5|      957|      1| 299.98|      299.98|
|       5|2013-07-25 00:00:...|11318|       COMPLETE|  11|      5|     1014|      2|  99.96|       49.98|
|       5|2013-07-25 00:00:...|11318|       COMPLETE|  10|      5|      365|      5| 299.95|       59.99|
|       5|2013-07-25 00:00:...|11318|       COMPLETE|   9|      5|      957|      1| 299.98|      299.98|
|       6|2013-07-25 00:00:...| 7130|       COMPLETE|null|   null|     null|   null|   null|        null|
|       7|2013-07-25 00:00:...| 4530|       COMPLETE|  16|      7|      926|      5|  79.95|       15.99|
|       7|2013-07-25 00:00:...| 4530|       COMPLETE|  15|      7|      957|      1| 299.98|      299.98|
|       7|2013-07-25 00:00:...| 4530|       COMPLETE|  14|      7|     1073|      1| 199.99|      199.99|
|       8|2013-07-25 00:00:...| 2911|     PROCESSING|  20|      8|      502|      1|   50.0|        50.0|
|       8|2013-07-25 00:00:...| 2911|     PROCESSING|  19|      8|     1014|      4| 199.92|       49.98|
+--------+--------------------+-----------------+---------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
only showing top 20 rows
```
As we can see, null values correspond to those orders that don't have anu order_item associated with them. So, to filter them what we have to do is no 
filter null values:

```python
>>> ordersLeftOuterJoin.where('order_item_order_id is null').show()
+--------+--------------------+-----------------+---------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|order_id|order_date|order_customer_id|   order_status|order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
+--------+--------------------+-----------------+---------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|       3|2013-07-25 00:00:...|12111|       COMPLETE|         null|   null|     null|   null|   null|        null|
|       6|2013-07-25 00:00:...| 7130|       COMPLETE|         null|   null|     null|   null|   null|        null|
|      22|2013-07-25 00:00:...|  333|       COMPLETE|         null|   null|     null|   null|   null|        null|
|      26|2013-07-25 00:00:...| 7562|       COMPLETE|         null|   null|     null|   null|   null|        null|
|      32|2013-07-25 00:00:...| 3960|       COMPLETE|         null|   null|     null|   null|   null|        null|
|      40|2013-07-25 00:00:...|12092|PENDING_PAYMENT|         null|   null|     null|   null|   null|        null|
|      47|2013-07-25 00:00:...| 8487|PENDING_PAYMENT|         null|   null|     null|   null|   null|        null|
|      53|2013-07-25 00:00:...| 4701|     PROCESSING|         null|   null|     null|   null|   null|        null|
|      54|2013-07-25 00:00:...|10628|PENDING_PAYMENT|         null|   null|     null|   null|   null|        null|
|      55|2013-07-25 00:00:...| 2052|        PENDING|         null|   null|     null|   null|   null|        null|
|      60|2013-07-25 00:00:...| 8365|PENDING_PAYMENT|         null|   null|     null|   null|   null|        null|
|      76|2013-07-25 00:00:...| 6898|       COMPLETE|         null|   null|     null|   null|   null|        null|
|      78|2013-07-25 00:00:...| 8619| PAYMENT_REVIEW|         null|   null|     null|   null|   null|        null|
|      79|2013-07-25 00:00:...| 7327|PENDING_PAYMENT|         null|   null|     null|   null|   null|        null|
|      80|2013-07-25 00:00:...| 3007|       COMPLETE|         null|   null|     null|   null|   null|        null|
|      82|2013-07-25 00:00:...| 3566|PENDING_PAYMENT|         null|   null|     null|   null|   null|        null|
|      85|2013-07-25 00:00:...| 1485|        PENDING|         null|   null|     null|   null|   null|        null|
|      86|2013-07-25 00:00:...| 6680|PENDING_PAYMENT|         null|   null|     null|   null|   null|        null|
|      89|2013-07-25 00:00:...|  824|        ON_HOLD|         null|   null|     null|   null|   null|        null|
|      90|2013-07-25 00:00:...| 9131|         CLOSED|         null|   null|     null|   null|   null|        null|
+--------+--------------------+-----------------+---------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
only showing top 20 rows
```
6. Check if there are any order_items where there ir no corresponding order in the orders dataset.
For that purpose we have to perform a right outer join if orders is on the right. In addition, we can perform a left outer join in case order is on the
left side of the join.

```python
orderItemsRightOuterJoin = orders. \
    join(orderItems,
         orders.order_id == orderItems.order_item_order_id,
         'right'---------> we could have used 'outer' as well)

+--------+--------------------+-----------------+---------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|order_id|order_date|order_customer_id|order_status|order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
+--------+--------------------+-----------------+---------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|       1|2013-07-25 00:00:...|11599|         CLOSED|   1|      1|      957|      1| 299.98|      299.98|
|       2|2013-07-25 00:00:...|  256|PENDING_PAYMENT|   2|      2|     1073|      1| 199.99|      199.99|
|       2|2013-07-25 00:00:...|  256|PENDING_PAYMENT|   3|      2|      502|      5|  250.0|        50.0|
|       2|2013-07-25 00:00:...|  256|PENDING_PAYMENT|   4|      2|      403|      1| 129.99|      129.99|
|       4|2013-07-25 00:00:...| 8827|         CLOSED|   5|      4|      897|      2|  49.98|       24.99|
|       4|2013-07-25 00:00:...| 8827|         CLOSED|   6|      4|      365|      5| 299.95|       59.99|
|       4|2013-07-25 00:00:...| 8827|         CLOSED|   7|      4|      502|      3|  150.0|        50.0|
|       4|2013-07-25 00:00:...| 8827|         CLOSED|   8|      4|     1014|      4| 199.92|       49.98|
|       5|2013-07-25 00:00:...|11318|       COMPLETE|   9|      5|      957|      1| 299.98|      299.98|
|       5|2013-07-25 00:00:...|11318|       COMPLETE|  10|      5|      365|      5| 299.95|       59.99|
|       5|2013-07-25 00:00:...|11318|       COMPLETE|  11|      5|     1014|      2|  99.96|       49.98|
|       5|2013-07-25 00:00:...|11318|       COMPLETE|  12|      5|      957|      1| 299.98|      299.98|
|       5|2013-07-25 00:00:...|11318|       COMPLETE|  13|      5|      403|      1| 129.99|      129.99|
|       7|2013-07-25 00:00:...| 4530|       COMPLETE|  14|      7|     1073|      1| 199.99|      199.99|
|       7|2013-07-25 00:00:...| 4530|       COMPLETE|  15|      7|      957|      1| 299.98|      299.98|
|       7|2013-07-25 00:00:...| 4530|       COMPLETE|  16|      7|      926|      5|  79.95|       15.99|
|       8|2013-07-25 00:00:...| 2911|     PROCESSING|  17|      8|      365|      3| 179.97|       59.99|
|       8|2013-07-25 00:00:...| 2911|     PROCESSING|  18|      8|      365|      5| 299.95|       59.99|
|       8|2013-07-25 00:00:...| 2911|     PROCESSING|  19|      8|     1014|      4| 199.92|       49.98|
|       8|2013-07-25 00:00:...| 2911|     PROCESSING|  20|      8|      502|      1|   50.0|        50.0|
+--------+--------------------+-----------------+---------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
only showing top 20 rows
```
To assure that we have well implemented both joins, we can show the difference between both joins. If it is imoplemented correctly, there will not be any
row differentiating between both datasets.

```python
orderItemsRightOuterJoin.filter('order_id is null').show()
>>> orderItemsRightOuterJoin.filter('order_id is null').show()
+--------+----------+-----------------+------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|order_id|order_date|order_customer_id|order_status|order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
+--------+----------+-----------------+------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
+--------+----------+-----------------+------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
```
As we can see, there is not any row. That means we have well calcullated the rows without correspondence in the other table.FullOuterJoin can only be perfomed in tables having a many to many relationship.



###GROUPING DATA AND PERFORMING AGGREGATIONS  
Many times we want to perform  aggregations such as sum, mimimum, maximum, etc within each group. We need to first group the data and them perform the aggregation.
- groupBy is the function which can be used to group the data on one or more columns
- once data is grouped we can perform all supported aggregations - sum, avg, min, max, etc.
- we can invoke the functions directly as part of the agg
- agg gives us more flexibility to give aliases to the derived fields.
Examples:
      
To see all the functions available, we disconnect from the pyspark2 console (CTRL + D) and then we execute 'spark-shell'

```python
scala> org.apache.spark.sql.functions. (and we enter TAB button). This functions will be shown:
scala> org.apache.spark.sql.functions.
abs   acos  add_months        approxCountDistinctarray array_contains    asInstanceOf 
asc   ascii asin  atan  atan2 avg   base64
    ...
```
Those are the functions that are available to be used in our spark pipeline:
```python
from pyspark.sql.functions import countDistinct
>>> from pyspark.sql.functions import countDistinct
>>> orders.select(countDistinct('order_status')).show()
+----------------------------+
|count(DISTINCT order_status)|
+----------------------------+
|                           9|
+----------------------------+
```
We can give the field an alias as well:

```python
>>> orders.select(countDistinct('order_status').alias('status_count')).show()
+------------+
|status_count|
+------------+
|9|
+------------+
```
Let's show order_items data:
```python
>>> orderItems.show()
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
| 1|      1|      957|      1| 299.98|      299.98|
| 2|      2|     1073|      1| 199.99|      199.99|
| 3|      2|      502|      5|  250.0|        50.0|
| 4|      2|      403|      1| 129.99|      129.99|
| 5|      4|      897|      2|  49.98|       24.99|
| 6|      4|      365|      5| 299.95|       59.99|
| 7|      4|      502|      3|  150.0|        50.0|
| 8|      4|     1014|      4| 199.92|       49.98|
| 9|      5|      957|      1| 299.98|      299.98|
|10|      5|      365|      5| 299.95|       59.99|
|11|      5|     1014|      2|  99.96|       49.98|
|12|      5|      957|      1| 299.98|      299.98|
|13|      5|      403|      1| 129.99|      129.99|
|14|      7|     1073|      1| 199.99|      199.99|
|15|      7|      957|      1| 299.98|      299.98|
|16|      7|      926|      5|  79.95|       15.99|
|17|      8|      365|      3| 179.97|       59.99|
|18|      8|      365|      5| 299.95|       59.99|
|19|      8|     1014|      4| 199.92|       49.98|
|20|      8|      502|      1|   50.0|        50.0|
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
only showing top 20 rows
```
1. Get revenue for each order_id from order items   
Now, to get revenue per product "2": 
**With SQL syntax**

```python
>>> orderItems. \
...    filter('order_item_order_id =2'). \
...    select(sum('order_item_subtotal')). \
...    show()
Traceback (most recent call last):
  File "<stdin>", line 3, in <module>
TypeError: unsupported operand type(s) for +: 'int' and 'str'
```

It fails because we have not imported the sql functions:
```python
from pyspark.sql.functions import sum
>>> orderItems. \
...    filter('order_item_order_id =2'). \
...    select(sum('order_item_subtotal')). \
...    show()
+------------------------+
|sum(order_item_subtotal)|
+------------------------+
|       579.9800109863281|
+------------------------+
```
If we want to round the result, we do:

```python
from pyspark.sql.functions import round
orderItems. \
...    filter('order_item_order_id =2'). \
...    select(round(sum('order_item_subtotal'),2)). \
...    show()
+----------------------------------+
|round(sum(order_item_subtotal), 2)|
+----------------------------------+
|    579.98                        |
+----------------------------------+
```

To group by a particular field, we must use the groupBy function before the aggregate function:
Let's get revenue per each orderId. Whenever we heard "for each" or "per key", we have to use the groupBy function and after the groupBy function
we can perform the operations.
```python
>>> orderItems. \
... groupBy('order_item_order_id') .\
... sum('order_item_subtotal') .\
... show()
+-------------------+------------------------+
|order_item_order_id|sum(order_item_subtotal)|
+-------------------+------------------------+
|    148            |      479.99000549316406|
|    463            |       829.9200096130371|
|    471            |      169.98000717163086|
|    496            |        441.950008392334|
|   1088            |      249.97000885009766|
|   1580            |      299.95001220703125|
|   1591            |       439.8599967956543|
|   1645            |       1509.790023803711|
|   2366            |       299.9700012207031|
|   2659            |       724.9100151062012|
|   2866            |        569.960018157959|
|   3175            |      209.97000122070312|
|   3749            |      143.97000122070312|
|   3794            |      299.95001220703125|
|   3918            |       829.9300155639648|
|   3997            |       579.9500122070312|
|   4101            |      129.99000549316406|
|   4519            |        79.9800033569336|
|   4818            |       399.9800109863281|
|   4900            |       179.9700050354004|
+--------------------------------------------+
only showing top 20 rows
```

As we can see, the sum of order revenue has not an intuitive name. For that purpose, we will create an alias:

```python
orderItems. \
... groupBy('order_item_order_id') .\
... sum('order_item_subtotal') .\    -----> we canot use the alias function then, because it will create an alias for the hole dataset. For that reason, 
        to create an alias we have to use the agg function.
```
We use the agg function:

```python
>>> orderItems. \
...    groupBy('order_item_order_id') .\
...    agg(sum('order_item_subtotal')).\
...    show()

+-------------------+------------------------+
|order_item_order_id|sum(order_item_subtotal)|
+-------------------+------------------------+
|    148|      479.99000549316406|
|    463|       829.9200096130371|
|    471|      169.98000717163086|
|    496|        441.950008392334|
|   1088|      249.97000885009766|
|   1580|      299.95001220703125|
|   1591|       439.8599967956543|
|   1645|       1509.790023803711|
|   2366|       299.9700012207031|
|   2659|       724.9100151062012|
|   2866|        569.960018157959|
|   3175|      209.97000122070312|
|   3749|      143.97000122070312|
|   3794|      299.95001220703125|
|   3918|       829.9300155639648|
|   3997|       579.9500122070312|
|   4101|      129.99000549316406|
|   4519|        79.9800033569336|
|   4818|       399.9800109863281|
|   4900|       179.9700050354004|
+--------------------------------+
only showing top 20 rows
```
The return value of the agg function is a column, not a dataset. So we can perform the alias on that column.
```python
>>> orderItems. \
...    groupBy('order_item_order_id') .\
...    agg(sum('order_item_subtotal').alias('order_revenue')).\
...    show()
+-------------------+------------------+
|order_item_order_id|     order_revenue|
+-------------------+------------------+
|    148            |479.99000549316406|
|    463            | 829.9200096130371|
|    471            |169.98000717163086|
|    496            |  441.950008392334|
|   1088            |249.97000885009766|
|   1580            |299.95001220703125|
|   1591            | 439.8599967956543|
|   1645            | 1509.790023803711|
|   2366            | 299.9700012207031|
|   2659            | 724.9100151062012|
|   2866            |  569.960018157959|
|   3175            |209.97000122070312|
|   3749            |143.97000122070312|
|   3794            |299.95001220703125|
|   3918            | 829.9300155639648|
|   3997            | 579.9500122070312|
|   4101            |129.99000549316406|
|   4519            |  79.9800033569336|
|   4818            | 399.9800109863281|
|   4900            | 179.9700050354004|
+-------------------+------------------+
only showing top 20 rows
```

And we round the result:
```python
>>> orderItems. \
...    groupBy('order_item_order_id') .\
...    agg(round(sum('order_item_subtotal'),2).alias('order_revenue')).\
...    show()
+-------------------+-------------+
|order_item_order_id|order_revenue|
+-------------------+-------------+
|    148            |       479.99|
|    463            |       829.92|
|    471            |       169.98|
|    496            |       441.95|
|   1088            |       249.97|
|   1580            |       299.95|
|   1591            |       439.86|
|   1645            |      1509.79|
|   2366            |       299.97|
|   2659            |       724.91|
|   2866            |       569.96|
|   3175            |       209.97|
|   3749            |       143.97|
|   3794            |       299.95|
|   3918            |       829.93|
|   3997            |       579.95|
|   4101            |       129.99|
|   4519            |        79.98|
|   4818            |       399.98|
|   4900            |       179.97|
+-------------------+-------------+
only showing top 20 rows
```
1. Get count by status from orders

```python
 orders.show()
+--------+--------------------+-----------------+---------------+   
|order_id|          order_date|order_customer_id|   order_status|
+--------+--------------------+-----------------+---------------+
|       1|2013-07-25 00:00:...|            11599|         CLOSED|
|       2|2013-07-25 00:00:...|              256|PENDING_PAYMENT|
|       3|2013-07-25 00:00:...|            12111|       COMPLETE|
|       4|2013-07-25 00:00:...|             8827|         CLOSED|
|       5|2013-07-25 00:00:...|            11318|       COMPLETE|
|       6|2013-07-25 00:00:...|             7130|       COMPLETE|
|       7|2013-07-25 00:00:...|             4530|       COMPLETE|
|       8|2013-07-25 00:00:...|             2911|     PROCESSING|
|       9|2013-07-25 00:00:...|             5657|PENDING_PAYMENT|
|      10|2013-07-25 00:00:...|             5648|PENDING_PAYMENT|
|      11|2013-07-25 00:00:...|              918| PAYMENT_REVIEW|
|      12|2013-07-25 00:00:...|             1837|         CLOSED|
|      13|2013-07-25 00:00:...|             9149|PENDING_PAYMENT|
|      14|2013-07-25 00:00:...|             9842|     PROCESSING|
|      15|2013-07-25 00:00:...|             2568|       COMPLETE|
|      16|2013-07-25 00:00:...|             7276|PENDING_PAYMENT|
|      17|2013-07-25 00:00:...|             2667|       COMPLETE|
|      18|2013-07-25 00:00:...|             1205|         CLOSED|
|      19|2013-07-25 00:00:...|             9488|PENDING_PAYMENT|
|      20|2013-07-25 00:00:...|             9198|     PROCESSING|
+--------+--------------------+-----------------+---------------+
only showing top 20 rows
```

In order to get the distinct order status values :
```python
>>> orders.select('order_status').distinct().show()
+---------------+   
|   order_status|
+---------------+
|PENDING_PAYMENT|
|       COMPLETE|
|        ON_HOLD|
| PAYMENT_REVIEW|
|     PROCESSING|
|         CLOSED|
|SUSPECTED_FRAUD|
|        PENDING|
|       CANCELED|
+---------------+
```
And I want to get count for each status:

```python
>>> orders.groupBy('order_status').count().show()

+---------------+-----+
|   order_status|count|
+---------------+-----+
|PENDING_PAYMENT|15030|
|       COMPLETE|22899|
|        ON_HOLD| 3798|
| PAYMENT_REVIEW|  729|
|     PROCESSING| 8275|
|         CLOSED| 7556|
|SUSPECTED_FRAUD| 1558|
|        PENDING| 7610|
|       CANCELED| 1428|
+---------------+-----+
```
As we can see, we don0t have any alias for the count column. For that reaso, we must use the agg function:
```python
from pyspark.sql.functions import count
orders.groupBy('order_status').agg(count('order_status').alias('status_count')).show()
+---------------+------------+  
|   order_status|status_count|
+---------------+------------+
|PENDING_PAYMENT|       15030|
|       COMPLETE|       22899|
|        ON_HOLD|        3798|
| PAYMENT_REVIEW|         729|
|     PROCESSING|        8275|
|         CLOSED|        7556|
|SUSPECTED_FRAUD|        1558|
|        PENDING|        7610|
|       CANCELED|        1428|
+---------------+------------+
```

3. Get daily product revenue (order_date and order_item_product_id are part of keys, order_item_subtotal is used for aggregation)      
We have to group the data by date (daily revenue) and by product_id.
Firstly, to perform aggregation by those two fields we have to join both tables.
If we preview the data we see:
```python
orders.show()
+--------+--------------------+-----------------+---------------+   
|order_id|order_date          |order_customer_id|   order_status|
+--------+--------------------+-----------------+---------------+
|       1|2013-07-25 00:00:...|11599            |         CLOSED|
|       2|2013-07-25 00:00:...|  256            |PENDING_PAYMENT|
|       3|2013-07-25 00:00:...|12111            |       COMPLETE|
|       4|2013-07-25 00:00:...| 8827            |         CLOSED|
|       5|2013-07-25 00:00:...|11318            |       COMPLETE|
|       6|2013-07-25 00:00:...| 7130            |       COMPLETE|
|       7|2013-07-25 00:00:...| 4530            |       COMPLETE|
|       8|2013-07-25 00:00:...| 2911            |     PROCESSING|
|       9|2013-07-25 00:00:...| 5657            |PENDING_PAYMENT|
|      10|2013-07-25 00:00:...| 5648            |PENDING_PAYMENT|
|      11|2013-07-25 00:00:...|  918            | PAYMENT_REVIEW|
|      12|2013-07-25 00:00:...| 1837            |         CLOSED|
|      13|2013-07-25 00:00:...| 9149            |PENDING_PAYMENT|
|      14|2013-07-25 00:00:...| 9842            |     PROCESSING|
|      15|2013-07-25 00:00:...| 2568            |       COMPLETE|
|      16|2013-07-25 00:00:...| 7276            |PENDING_PAYMENT|
|      17|2013-07-25 00:00:...| 2667            |       COMPLETE|
|      18|2013-07-25 00:00:...| 1205            |         CLOSED|
|      19|2013-07-25 00:00:...| 9488            |PENDING_PAYMENT|
|      20|2013-07-25 00:00:...| 9198            |     PROCESSING|
+--------+--------------------+-----------------+---------------+
only showing top 20 rows
```
In order table we will need the "order_date" column
In order_items table we will need to take order_item_product_id column as well as order_item_subtotal
```python
orderItems.show()
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
| 1|      1|      957|     1.0| 299.98|      299.98|
| 2|      2|     1073|     1.0| 199.99|      199.99|
| 3|      2|      502|     5.0|  250.0|        50.0|
| 4|      2|      403|     1.0| 129.99|      129.99|
| 5|      4|      897|     2.0|  49.98|       24.99|
| 6|      4|      365|     5.0| 299.95|       59.99|
| 7|      4|      502|     3.0|  150.0|        50.0|
| 8|      4|     1014|     4.0| 199.92|       49.98|
| 9|      5|      957|     1.0| 299.98|      299.98|
|10|      5|      365|     5.0| 299.95|       59.99|
|11|      5|     1014|     2.0|  99.96|       49.98|
|12|      5|      957|     1.0| 299.98|      299.98|
|13|      5|      403|     1.0| 129.99|      129.99|
|14|      7|     1073|     1.0| 199.99|      199.99|
|15|      7|      957|     1.0| 299.98|      299.98|
|16|      7|      926|     5.0|  79.95|       15.99|
|17|      8|      365|     3.0| 179.97|       59.99|
|18|      8|      365|     5.0| 299.95|       59.99|
|19|      8|     1014|     4.0| 199.92|       49.98|
|20|      8|      502|     1.0|   50.0|        50.0|
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
only showing top 20 rows
```

So as to join both tables, we will use order_id column (from order table) and order_items_order_id (from orderItems table).
```python
>>> ordersJoin = orders. \
    join(orderItems, orders.order_id == orderItems.order_item_order_id)
    >>> ordersJoin.printSchema()
    root
     |-- order_id: integer (nullable = true)
     |-- order_date: string (nullable = true)
     |-- order_customer_id: integer (nullable = true)
     |-- order_status: string (nullable = true)
     |-- order_item_id: integer (nullable = true)
     |-- order_item_order_id: integer (nullable = true)
     |-- order_item_product_id: integer (nullable = true)
     |-- order_item_quantity: float (nullable = true)
     |-- order_item_subtotal: float (nullable = true)
     |-- order_item_product_price: float (nullable = true)


>>> ordersJoin.show()
+--------+--------------------+-----------------+---------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|order_id|order_date|order_customer_idorder_status|order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|+--------+--------------------+-----------------+---------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|       1|2013-07-25 00:00:...|11599|         CLOSED| 1|      1|      957|    1.0| 299.98|      299.98|
|       2|2013-07-25 00:00:...|  256|PENDING_PAYMENT| 2|      2|     1073|    1.0| 199.99|      199.99|
|       2|2013-07-25 00:00:...|  256|PENDING_PAYMENT| 3|      2|      502|    5.0|  250.0|        50.0|
|       2|2013-07-25 00:00:...|  256|PENDING_PAYMENT| 4|      2|      403|    1.0| 129.99|      129.99|
|       4|2013-07-25 00:00:...| 8827|         CLOSED| 5|      4|      897|    2.0|  49.98|       24.99|
|       4|2013-07-25 00:00:...| 8827|         CLOSED| 6|      4|      365|    5.0| 299.95|       59.99|
|       4|2013-07-25 00:00:...| 8827|         CLOSED| 7|      4|      502|    3.0|  150.0|        50.0|
|       4|2013-07-25 00:00:...| 8827|         CLOSED| 8|      4|     1014|    4.0| 199.92|       49.98|
|       5|2013-07-25 00:00:...|11318|       COMPLETE| 9|      5|      957|    1.0| 299.98|      299.98|
|       5|2013-07-25 00:00:...|11318|       COMPLETE|10|      5|      365|    5.0| 299.95|       59.99|
|       5|2013-07-25 00:00:...|11318|       COMPLETE|11|      5|     1014|    2.0|  99.96|       49.98|
|       5|2013-07-25 00:00:...|11318|       COMPLETE|12|      5|      957|    1.0| 299.98|      299.98|
|       5|2013-07-25 00:00:...|11318|       COMPLETE|13|      5|      403|    1.0| 129.99|      129.99|
|       7|2013-07-25 00:00:...| 4530|       COMPLETE|14|      7|     1073|    1.0| 199.99|      199.99|
|       7|2013-07-25 00:00:...| 4530|       COMPLETE|15|      7|      957|    1.0| 299.98|      299.98|
|       7|2013-07-25 00:00:...| 4530|       COMPLETE|16|      7|      926|    5.0|  79.95|       15.99|
|       8|2013-07-25 00:00:...| 2911|     PROCESSING|17|      8|      365|    3.0| 179.97|       59.99|
|       8|2013-07-25 00:00:...| 2911|     PROCESSING|18|      8|      365|    5.0| 299.95|       59.99|
|       8|2013-07-25 00:00:...| 2911|     PROCESSING|19|      8|     1014|    4.0| 199.92|       49.98|
|       8|2013-07-25 00:00:...| 2911|     PROCESSING|20|      8|      502|    1.0|   50.0|        50.0|
+--------+--------------------+-----------------+---------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
only showing top 20 rows
```
From the joined tables , we must use order_date,order_item_product_id and order_item_subtotal.
As we have to group by date and by product_id, we have to create a composite groupBy:
```python
from pyspark.sql.functions import sum, round 
>>> ordersJoin. \
...     groupBy('order_date', 'order_item_product_id'). \
...     agg(sum('order_item_subtotal').alias('product_revenue')).show()
+--------------------+---------------------+------------------+
|order_date          |order_item_product_id|   product_revenue|
+--------------------+---------------------+------------------+
|2013-07-27 00:00:...|      703            | 99.95000076293945|
|2013-07-29 00:00:...|      793            |44.970001220703125|
|2013-08-04 00:00:...|      825            | 95.97000122070312|
|2013-08-06 00:00:...|       93            |24.989999771118164|
|2013-08-12 00:00:...|      627            | 5358.660064697266|
|2013-08-15 00:00:...|      926            |15.989999771118164|
|2013-08-17 00:00:...|      116            | 224.9499969482422|
|2013-09-04 00:00:...|      957            | 7499.500274658203|
|2013-09-07 00:00:...|      235            |104.97000122070312|
|2013-09-17 00:00:...|      792            | 89.93999671936035|
|2013-09-25 00:00:...|       44            | 359.9400100708008|
|2013-09-27 00:00:...|      276            |31.989999771118164|
|2013-09-28 00:00:...|      572            |119.97000122070312|
|2013-10-04 00:00:...|      792            |44.970001220703125|
|2013-10-05 00:00:...|      886            |24.989999771118164|
|2013-10-14 00:00:...|      835            |31.989999771118164|
|2013-10-16 00:00:...|      835            |31.989999771118164|
|2013-10-19 00:00:...|     1004            |18399.080505371094|
|2013-11-06 00:00:...|      502            |            9750.0|
|2013-11-12 00:00:...|      282            | 63.97999954223633|
+--------------------+---------------------+------------------+
only showing top 20 rows

>>> ordersJoin. \
...     groupBy('order_date', 'order_item_product_id'). \
...     agg(round(sum('order_item_subtotal'),2).alias('product_revenue')).show()

+--------------------+---------------------+---------------+
|order_date          |order_item_product_id|product_revenue |
+--------------------+---------------------+---------------+
|2013-07-27 00:00:...|      703             |          99.95|
|2013-07-29 00:00:...|      793             |          44.97|
|2013-08-04 00:00:...|      825             |          95.97|
|2013-08-06 00:00:...|       93             |          24.99|
|2013-08-12 00:00:...|      627             |        5358.66|
|2013-08-15 00:00:...|      926             |          15.99|
|2013-08-17 00:00:...|      116             |         224.95|
|2013-09-04 00:00:...|      957             |         7499.5|
|2013-09-07 00:00:...|      235             |         104.97|
|2013-09-17 00:00:...|      792             |          89.94|
|2013-09-25 00:00:...|       44             |         359.94|
|2013-09-27 00:00:...|      276             |          31.99|
|2013-09-28 00:00:...|      572             |         119.97|
|2013-10-04 00:00:...|      792             |          44.97|
|2013-10-05 00:00:...|      886             |          24.99|
|2013-10-14 00:00:...|      835             |          31.99|
|2013-10-16 00:00:...|      835             |          31.99|
|2013-10-19 00:00:...|     1004             |       18399.08|
|2013-11-06 00:00:...|      502             |         9750.0|
|2013-11-12 00:00:...|      282             |          63.98|
+--------------------+---------------------+---------------+
only showing top 20 rows
```
###SORTING DATA
We can sort the data coming from an aggregation:

- sort or orderBy can be used to sort the data globally.
- we can get the help by doing help(orders.sort) and help(orders.orderBy)


**Example 1:** 
get orders in ascending order by date
```python
orders.sort('order_date').show()
+--------+--------------------+-----------------+---------------+
|order_id|  order_date        |order_customer_id|   order_status|
+--------+--------------------+-----------------+---------------+
|       1|2013-07-25 00:00:...|            11599|         CLOSED|
|       2|2013-07-25 00:00:...|              256|PENDING_PAYMENT|
|       3|2013-07-25 00:00:...|            12111|       COMPLETE|
|       4|2013-07-25 00:00:...|             8827|         CLOSED|
|       5|2013-07-25 00:00:...|            11318|       COMPLETE|
|       6|2013-07-25 00:00:...|             7130|       COMPLETE|
|       7|2013-07-25 00:00:...|             4530|       COMPLETE|
|       8|2013-07-25 00:00:...|             2911|     PROCESSING|
|       9|2013-07-25 00:00:...|             5657|PENDING_PAYMENT|
|      10|2013-07-25 00:00:...|             5648|PENDING_PAYMENT|
|      11|2013-07-25 00:00:...|              918| PAYMENT_REVIEW|
|      12|2013-07-25 00:00:...|             1837|         CLOSED|
|      13|2013-07-25 00:00:...|             9149|PENDING_PAYMENT|
|      14|2013-07-25 00:00:...|             9842|     PROCESSING|
|      15|2013-07-25 00:00:...|             2568|       COMPLETE|
|      16|2013-07-25 00:00:...|             7276|PENDING_PAYMENT|
|      17|2013-07-25 00:00:...|             2667|       COMPLETE|
|      18|2013-07-25 00:00:...|             1205|         CLOSED|
|      19|2013-07-25 00:00:...|             9488|PENDING_PAYMENT|
|      20|2013-07-25 00:00:...|             9198|     PROCESSING|
+--------+--------------------+-----------------+---------------+
only showing top 20 rows
```

Now, if we want to order , in ascending order, by two columns, we have to perform a composite "orderBy":
```python
orders.sort('order_date','order_customer_id').show()
+--------+--------------------+-----------------+---------------+
|order_id|  order_date        |order_customer_id|   order_status|
+--------+--------------------+-----------------+---------------+
|   57762|2013-07-25 00:00:...|              192|       COMPLETE|
|      29|2013-07-25 00:00:...|              196|     PROCESSING|
|   57759|2013-07-25 00:00:...|              216|        ON_HOLD|
|       2|2013-07-25 00:00:...|              256|PENDING_PAYMENT|
|   57782|2013-07-25 00:00:...|              284|         CLOSED|
|      22|2013-07-25 00:00:...|              333|       COMPLETE|
|      28|2013-07-25 00:00:...|              656|       COMPLETE|
|      74|2013-07-25 00:00:...|              662|PENDING_PAYMENT|
|      81|2013-07-25 00:00:...|              674|     PROCESSING|
|      89|2013-07-25 00:00:...|              824|        ON_HOLD|
|      11|2013-07-25 00:00:...|              918| PAYMENT_REVIEW|
|   57778|2013-07-25 00:00:...|             1143|        PENDING|
|      63|2013-07-25 00:00:...|             1148|       COMPLETE|
|      18|2013-07-25 00:00:...|             1205|         CLOSED|
|      83|2013-07-25 00:00:...|             1265|       COMPLETE|
|   57767|2013-07-25 00:00:...|             1338|PENDING_PAYMENT|
|      67|2013-07-25 00:00:...|             1406|       COMPLETE|
|      85|2013-07-25 00:00:...|             1485|        PENDING|
|      46|2013-07-25 00:00:...|             1549|        ON_HOLD|
|   57764|2013-07-25 00:00:...|             1763|       COMPLETE|
+--------+--------------------+-----------------+---------------+
```

If we want to perform an orderBy by different criteria (order_date in descending order and order_customer_id in ascending order) in some of the columns, we will do it in the next way(1 means ordering in ascending mode)


```python
orders.sort(['order_date', 'order_customer_id'], ascending = [0,1]).show()
+--------+--------------------+-----------------+---------------+
|order_id|  order_date        |order_customer_id|   order_status|
+--------+--------------------+-----------------+---------------+
|   57617|2014-07-24 00:00:...|                3|       COMPLETE|
|   57733|2014-07-24 00:00:...|               17|         CLOSED|
|   57684|2014-07-24 00:00:...|               98|       COMPLETE|
|   57695|2014-07-24 00:00:...|              112|       COMPLETE|
|   57598|2014-07-24 00:00:...|              138|        PENDING|
|   57715|2014-07-24 00:00:...|              243|PENDING_PAYMENT|
|   67396|2014-07-24 00:00:...|              282|PENDING_PAYMENT|
|   67406|2014-07-24 00:00:...|              476|       COMPLETE|
|   67393|2014-07-24 00:00:...|              585|     PROCESSING|
|   57675|2014-07-24 00:00:...|              595|PENDING_PAYMENT|
|   57729|2014-07-24 00:00:...|              611|PENDING_PAYMENT|
|   57658|2014-07-24 00:00:...|              629|PENDING_PAYMENT|
|   57696|2014-07-24 00:00:...|              656|PENDING_PAYMENT|
|   57749|2014-07-24 00:00:...|              666|         CLOSED|
|   57728|2014-07-24 00:00:...|              822|         CLOSED|
|   57680|2014-07-24 00:00:...|             1000|         CLOSED|
|   57601|2014-07-24 00:00:...|             1046|        ON_HOLD|
|   67408|2014-07-24 00:00:...|             1150|     PROCESSING|
|   57748|2014-07-24 00:00:...|             1167|       COMPLETE|
|   57636|2014-07-24 00:00:...|             1211|        PENDING|
+--------+--------------------+-----------------+---------------+
only showing top 20 rows
```

or, we can do the same in the following way:
```python
orders.sort('order_date', orders.order_customer_id.desc()).show()
+--------+--------------------+-----------------+---------------+
|order_id|          order_date|order_customer_id|   order_status|
+--------+--------------------+-----------------+---------------+
|   57785|2013-07-25 00:00:...|            12347|     PROCESSING|
|   57787|2013-07-25 00:00:...|            12294|PENDING_PAYMENT|
|      51|2013-07-25 00:00:...|            12271|         CLOSED|
|     103|2013-07-25 00:00:...|            12256|     PROCESSING|
|      48|2013-07-25 00:00:...|            12186|     PROCESSING|
|     100|2013-07-25 00:00:...|            12131|     PROCESSING|
|       3|2013-07-25 00:00:...|            12111|       COMPLETE|
|      40|2013-07-25 00:00:...|            12092|PENDING_PAYMENT|
|   57779|2013-07-25 00:00:...|            11941|       COMPLETE|
|      70|2013-07-25 00:00:...|            11809|PENDING_PAYMENT|
|      59|2013-07-25 00:00:...|            11644|PENDING_PAYMENT|
|       1|2013-07-25 00:00:...|            11599|         CLOSED|
|      94|2013-07-25 00:00:...|            11589|     PROCESSING|
|      38|2013-07-25 00:00:...|            11586|     PROCESSING|
|      99|2013-07-25 00:00:...|            11542|PENDING_PAYMENT|
|      24|2013-07-25 00:00:...|            11441|         CLOSED|
|       5|2013-07-25 00:00:...|            11318|       COMPLETE|
|   57774|2013-07-25 00:00:...|            11196|PENDING_PAYMENT|
|   67416|2013-07-25 00:00:...|            10920|         CLOSED|
|      97|2013-07-25 00:00:...|            10784|        PENDING|
+--------+--------------------+-----------------+---------------+
only showing top 20 rows
```
- By default, data will be sorted in ascending order.
- We can change the order by using the orderBy using desc function.
- At times, we might not want to sort the data globally. Instead, we might want to sort the data within a group. In that case, we can use sortWithinPartitions (for example to sort the stores by revenue within each state):
**Example 2**
Say we want to order the data by date and then, we want to order the data by order_customer_id

```python
    >>> orders.show()
    +--------+--------------------+-----------------+---------------+       
    |order_id|order_date          |order_customer_id|   order_status|
    +--------+--------------------+-----------------+---------------+
    |       1|2013-07-25 00:00:...|             11599|         CLOSED|
    |       2|2013-07-25 00:00:...|               256|PENDING_PAYMENT|
    |       3|2013-07-25 00:00:...|             12111|       COMPLETE|
    |       4|2013-07-25 00:00:...|              8827|         CLOSED|
    |       5|2013-07-25 00:00:...|             11318|       COMPLETE|
    |       6|2013-07-25 00:00:...|              7130|       COMPLETE|
    |       7|2013-07-25 00:00:...|              4530|       COMPLETE|
    |       8|2013-07-25 00:00:...|              2911|     PROCESSING|
    |       9|2013-07-25 00:00:...|              5657|PENDING_PAYMENT|
    |      10|2013-07-25 00:00:...|              5648|PENDING_PAYMENT|
    |      11|2013-07-25 00:00:...|               918| PAYMENT_REVIEW|
    |      12|2013-07-25 00:00:...|              1837|         CLOSED|
    |      13|2013-07-25 00:00:...|              9149|PENDING_PAYMENT|
    |      14|2013-07-25 00:00:...|              9842|     PROCESSING|
    |      15|2013-07-25 00:00:...|              2568|       COMPLETE|
    |      16|2013-07-25 00:00:...|              7276|PENDING_PAYMENT|
    |      17|2013-07-25 00:00:...|              2667|       COMPLETE|
    |      18|2013-07-25 00:00:...|              1205|         CLOSED|
    |      19|2013-07-25 00:00:...|              9488|PENDING_PAYMENT|
    |      20|2013-07-25 00:00:...|              9198|     PROCESSING|
    +--------+--------------------+-----------------+---------------+
    only showing top 20 rows


    >>> orders.sortWithinPartitions('order_date','order_customer_id').show()
    +--------+--------------------+-----------------+---------------+
    |order_id|          order_date|order_customer_id|   order_status|
    +--------+--------------------+-----------------+---------------+
    |   57762|2013-07-25 00:00:...|      192        |       COMPLETE|
    |      29|2013-07-25 00:00:...|      196        |     PROCESSING|
    |   57759|2013-07-25 00:00:...|      216        |        ON_HOLD|
    |       2|2013-07-25 00:00:...|      256        |PENDING_PAYMENT|
    |   57782|2013-07-25 00:00:...|      284        |         CLOSED|
    |      22|2013-07-25 00:00:...|      333        |       COMPLETE|
    |      28|2013-07-25 00:00:...|      656        |       COMPLETE|
    |      74|2013-07-25 00:00:...|      662        |PENDING_PAYMENT|
    |      81|2013-07-25 00:00:...|      674        |     PROCESSING|
    |      89|2013-07-25 00:00:...|      824        |        ON_HOLD|
    |      11|2013-07-25 00:00:...|      918        | PAYMENT_REVIEW|
    |   57778|2013-07-25 00:00:...|     1143        |        PENDING|
    |      63|2013-07-25 00:00:...|     1148        |       COMPLETE|
    |      18|2013-07-25 00:00:...|     1205        |         CLOSED|
    |      83|2013-07-25 00:00:...|     1265        |       COMPLETE|
    |   57767|2013-07-25 00:00:...|     1338        |PENDING_PAYMENT|
    |      67|2013-07-25 00:00:...|     1406        |       COMPLETE|
    |      85|2013-07-25 00:00:...|     1485        |        PENDING|
    |      46|2013-07-25 00:00:...|     1549        |        ON_HOLD|
    |   57764|2013-07-25 00:00:...|     1763        |       COMPLETE|
    +--------+--------------------+-----------------+---------------+
    only showing top 20 rows
```
The difference between sort and sortWithinPartitions is that sort function sorts the data globally and sortWithinPartitions does not

**Example 3**
Sort orders by status.    

```python
orders.sort('order_status').show()
+--------+--------------------+-----------------+------------+
|order_id|          Order_date|order_customer_id|order_status|
+--------+--------------------+-----------------+------------+
|     527|2013-07-28 00:00:...|             5426|    CANCELED|
|    1435|2013-08-01 00:00:...|             1879|    CANCELED|
|     552|2013-07-28 00:00:...|             1445|    CANCELED|
|     112|2013-07-26 00:00:...|             5375|    CANCELED|
|     564|2013-07-28 00:00:...|             2216|    CANCELED|
|     955|2013-07-30 00:00:...|             8117|    CANCELED|
|    1383|2013-08-01 00:00:...|             1753|    CANCELED|
|     962|2013-07-30 00:00:...|             9492|    CANCELED|
|     607|2013-07-28 00:00:...|             6376|    CANCELED|
|    1013|2013-07-30 00:00:...|             1903|    CANCELED|
|     667|2013-07-28 00:00:...|             4726|    CANCELED|
|    1169|2013-07-31 00:00:...|             3971|    CANCELED|
|     717|2013-07-29 00:00:...|             8208|    CANCELED|
|    1186|2013-07-31 00:00:...|            11947|    CANCELED|
|     753|2013-07-29 00:00:...|             5094|    CANCELED|
|    1190|2013-07-31 00:00:...|            12360|    CANCELED|
|      50|2013-07-25 00:00:...|             5225|    CANCELED|
|    1313|2013-08-01 00:00:...|             3471|    CANCELED|
|     716|2013-07-29 00:00:...|             2581|    CANCELED|
|    1365|2013-08-01 00:00:...|             8567|    CANCELED|
+--------+--------------------+-----------------+------------+
only showing top 20 rows
```
We can see that data is ordered by order_status in ascending order

**Example 4**
- sort orders by date and then by status (in ascending way both fields).    

```python
orders.sort('order_date','order_status').show()
+--------+--------------------+-----------------+------------+
|order_id|          order_date|order_customer_id|order_status|
+--------+--------------------+-----------------+------------+
|      50|2013-07-25 00:00:...|             5225|    CANCELED|
|       1|2013-07-25 00:00:...|            11599|      CLOSED|
|      12|2013-07-25 00:00:...|             1837|      CLOSED|
|       4|2013-07-25 00:00:...|             8827|      CLOSED|
|      37|2013-07-25 00:00:...|             5863|      CLOSED|
|      18|2013-07-25 00:00:...|             1205|      CLOSED|
|      24|2013-07-25 00:00:...|            11441|      CLOSED|
|      25|2013-07-25 00:00:...|             9503|      CLOSED|
|   57754|2013-07-25 00:00:...|             4648|      CLOSED|
|      90|2013-07-25 00:00:...|             9131|      CLOSED|
|      51|2013-07-25 00:00:...|            12271|      CLOSED|
|      57|2013-07-25 00:00:...|             7073|      CLOSED|
|      61|2013-07-25 00:00:...|             4791|      CLOSED|
|      62|2013-07-25 00:00:...|             9111|      CLOSED|
|      87|2013-07-25 00:00:...|             3065|      CLOSED|
|     101|2013-07-25 00:00:...|             5116|      CLOSED|
|   57766|2013-07-25 00:00:...|             2376|      CLOSED|
|   57781|2013-07-25 00:00:...|             6143|      CLOSED|
|   57782|2013-07-25 00:00:...|              284|      CLOSED|
|   67416|2013-07-25 00:00:...|            10920|      CLOSED|
+--------+--------------------+-----------------+------------+
only showing top 20 rows
```
If we want to sort the data where one of the fields is in descending mode:

```python
orders.sort(orders.order_date.desc(),'order_status').show()
+--------+--------------------+-----------------+------------+
|order_id|          order_date|order_customer_id|order_status|
+--------+--------------------+-----------------+------------+
|   57638|2014-07-24 00:00:...|             8905|    CANCELED|
|   57672|2014-07-24 00:00:...|            10855|    CANCELED|
|   57631|2014-07-24 00:00:...|             3728|      CLOSED|
|   57628|2014-07-24 00:00:...|             6463|      CLOSED|
|   57616|2014-07-24 00:00:...|             6545|      CLOSED|
|   57642|2014-07-24 00:00:...|            10258|      CLOSED|
|   57613|2014-07-24 00:00:...|             9278|      CLOSED|
|   57680|2014-07-24 00:00:...|             1000|      CLOSED|
|   57607|2014-07-24 00:00:...|             2666|      CLOSED|
|   57690|2014-07-24 00:00:...|             2697|      CLOSED|
|   57649|2014-07-24 00:00:...|             5944|      CLOSED|
|   57654|2014-07-24 00:00:...|             2980|      CLOSED|
|   57674|2014-07-24 00:00:...|             6734|      CLOSED|
|   57683|2014-07-24 00:00:...|             4337|      CLOSED|
|   57685|2014-07-24 00:00:...|            10988|      CLOSED|
|   57686|2014-07-24 00:00:...|             2081|      CLOSED|
|   57708|2014-07-24 00:00:...|             4557|      CLOSED|
|   57709|2014-07-24 00:00:...|             5446|      CLOSED|
|   57711|2014-07-24 00:00:...|             7783|      CLOSED|
|   57717|2014-07-24 00:00:...|             1564|      CLOSED|
+--------+--------------------+-----------------+------------+
only showing top 20 rows
```

**Example 5**
sort orderItems by orde_item_order_id and order_item_subtotal descending
```python
orderItems.sort('order_item_order_id', orderItems.order_item_subtotal.desc()).show()
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|           1|                   1|                 957 |                1.0|             299.98|                  299.98|
|           3|                   2|                  502|                5.0|              250.0|                    50.0|
|           2|                   2|                 1073|                1.0|             199.99|                  199.99|
|           4|                   2|                  403|               1.0|              129.99|                  129.99|
|           6|                   4|                  365|               5.0|              299.95|                   59.99|
|           8|                   4|                 1014|               4.0|              199.92|                   49.98|
|           7|                   4|                  502|               3.0|               150.0|                    50.0|
|           5|                   4|                  897|               2.0|               49.98|                   24.99|
|           9|                   5|                  957|               1.0|              299.98|                  299.98|
|          12|                   5|                  957|               1.0|              299.98|                  299.98|
|          10|                   5|                  365|               5.0|              299.95|                   59.99|
|          13|                   5|                  403|               1.0|              129.99|                  129.99|
|          11|                   5|                 1014|               2.0|               99.96|                   49.98|
|          15|                   7|                  957|               1.0|              299.98|                  299.98|
|          14|                   7|                 1073|               1.0|              199.99|                  199.99|
|          16|                   7|                  926|               5.0|                79.95|                  15.99|
|           18|                 8|                  365|                5.0|                99.95|                  59.99|
|           19|                 8|                  1014|               4.0|                199.92|                 49.98|
|           17|                 8|                  365|                3.0|                179.97|                 59.99|
|           20|                 8|                  502|                1.0|                50.0|                   50.0|
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
only showing top 20 rows
```
or we can do the same in the following way:

```python
orderItems.sort(['order_item_order_id', 'order_item_subtotal'],ascending = [1,0]).show()
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
|           1|                  1|                  957|                1.0|            299.98|                     299.98|
|           3|                  2|                  502|                5.0|            250.0|                      50.0|
|           2|                  2|                  1073|               1.0|            199.99|                     199.99|
|           4|                  2|                  403|                1.0|            129.99|                     129.99|
|           6|                  4|                  365|                5.0|            299.95|                     59.99|
|           8|                  4|                  1014|               4.0|            199.92|                     49.98|
|           7|                  4|                  502|                3.0|             150.0|                     50.0|
|           9|                  5|                  957|                1.0|            299.98|                     299.98|
|           12|                 5|                  57|                 1.0|            299.98|                     299.98|
|           10|                 5|              365|5.0|             299.95|             59.99| 
|           13|                 5|                  403|                1.0|            129.99|                     129.99|
|           11|                 5|                  1014|               2.0|             99.96|                     49.98|
|           15|                 7|                  957|                1.0|            299.98|                     299.98|
|           14|                 7|                  1073|               1.0|            199.99|                     199.99|
|           16|                 7|                  926|                5.0|             79.95|                     15.99|
|           18|                 8|                  365|                5.0|            299.95|                     59.99|
|           19|                 8|                  1014|               4.0|            199.92|                     49.98|
|           17|                 8|                  365|                3.0|            179.97|                     59.99|
|           20|                 8|                  502|                1.0|             50.0|                      50.0|
+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
only showing top 20 rows
```

**Example 6**
Take daily product revenue data and sort in ascending order by date and then descending order by revenue:

```
from pyspark.sql.functions import sum, round

>>> dailyRevenue = orders.where('order_status in ("COMPLETE", "CLOSED")').\
...     join(orderItems, orders.order_id == orderItems.order_item_order_id).\
...     groupBy(orders.order_date, orderItems.order_item_product_id).\
...     agg(round(sum(orderItems.order_item_subtotal),2).alias('revenue'))
dailyRevenue.show()
+--------------------+---------------------+-------+
|  order_date        | order_item_product_id|revenue|
+--------------------+---------------------+-------+
|2013-07-27 00:00:...|                  703|  39.98|
|2013-07-29 00:00:...|                  793|  44.97|
|2013-08-12 00:00:...|                  627| 3199.2|
|2013-08-15 00:00:...|                  926|  15.99|
|2013-09-04 00:00:...|                  957|3599.76|
|2013-09-07 00:00:...|                  235| 104.97|
|2013-09-17 00:00:...|                  792|  14.99|
|2013-09-25 00:00:...|                   44| 239.96|
|2013-09-27 00:00:...|                  276|  31.99|
|2013-10-04 00:00:...|                  792|  44.97|
|2013-10-14 00:00:...|                  835|  31.99|
|2013-10-19 00:00:...|                 1004|5599.72|
|2013-11-06 00:00:...|                  502| 3800.0|
|2013-11-12 00:00:...|                  282|  63.98|
|2013-11-19 00:00:...|                  792|  59.96|
|2013-11-29 00:00:...|                  235| 174.95|
|2013-11-30 00:00:...|                  278|  44.99|
|2013-12-03 00:00:...|                  778|  99.96|
|2013-12-09 00:00:...|                  564|  180.0|
|2013-12-11 00:00:...|                  775|  49.95|
+--------------------+---------------------+-------+
```

Now, we have to do the sorting part:
```python
dailyRevenue.sort('order_date', dailyRevenue.revenue.desc()).show()
+--------------------+---------------------+-------+    
|  Order_date        |order_item_product_id|revenue|
+--------------------+---------------------+-------+
|2013-07-25 00:00:...|                 1004|5599.72|
|2013-07-25 00:00:...|                  191|5099.49|
|2013-07-25 00:00:...|                  957| 4499.7|
|2013-07-25 00:00:...|                  365|3359.44|
|2013-07-25 00:00:...|                  1073|2999.85|
|2013-07-25 00:00:...|                  1014|2798.88|
|2013-07-25 00:00:...|                  403|1949.85|
|2013-07-25 00:00:...|                  502| 1650.0|
|2013-07-25 00:00:...|                  627|1079.73|
|2013-07-25 00:00:...|                  226| 599.99|
|2013-07-25 00:00:...|                   24| 319.96|
|2013-07-25 00:00:...|                  821| 207.96|
|2013-07-25 00:00:...|                  625| 199.99|
|2013-07-25 00:00:...|                  705| 119.99|
|2013-07-25 00:00:...|                  572| 119.97|
|2013-07-25 00:00:...|                  666| 109.99|
|2013-07-25 00:00:...|                  725|  108.0|
|2013-07-25 00:00:...|                  134|  100.0|
|2013-07-25 00:00:...|                  906|  99.96|
|2013-07-25 00:00:...|                  828|  95.97|
+--------------------+---------------------+-------+
only showing top 20 rows
```

We can do the same in the following way:

```python
dailyRevenue.sort(['order_date','revenue'], ascending = [1,0]).show()
+--------------------+---------------------+-------+
|  Order_date        |order_item_product_id|revenue|
+--------------------+---------------------+-------+
|2013-07-25 00:00:...|                  1004|5599.72|
|2013-07-25 00:00:...|                  191|5099.49|
|2013-07-25 00:00:...|                  957| 4499.7|
|2013-07-25 00:00:...|                  365|3359.44|
|2013-07-25 00:00:...|                  1073|2999.85|
|2013-07-25 00:00:...|                  1014|2798.88|
|2013-07-25 00:00:...|                  403|1949.85|
|2013-07-25 00:00:...|                  502| 1650.0|
|2013-07-25 00:00:...|                  627|1079.73|
|2013-07-25 00:00:...|                  226| 599.99|
|2013-07-25 00:00:...|                   24| 319.96|
|2013-07-25 00:00:...|                  821| 207.96|
|2013-07-25 00:00:...|                  625| 199.99|
|2013-07-25 00:00:...|                  705| 119.99|
|2013-07-25 00:00:...|                  572| 119.97|
|2013-07-25 00:00:...|                  666| 109.99|
|2013-07-25 00:00:...|                  725|  108.0|
|2013-07-25 00:00:...|                  134|  100.0|
|2013-07-25 00:00:...|                  906|  99.96|
|2013-07-25 00:00:...|                  828|  95.97|
+--------------------+---------------------+-------+
only showing top 20 rows
```

Now, if we want it to sort the data by using exactly 2 threads, what I must do is:
```python
spark.conf.set('spark.sql.shuffle.partitions','2')
```

###APACHE SPARK 2.X


**PROCESSING DATA USING DATAFRAMES - WINDOW FUNCTIONS**

Why using "PARTITION BY" in spite of groupBy? Because when we use groupBy there is a limitation of either use the select field as part  the group clause
(when data is grouped by a field, that field must takes part of the fields selected) or using an aggregation function such as sum, min, avg on a numeric field.

```python
 employeesPath = '/public/hr_db/employees'
 employees = spark. \
...     read. \
...     format('csv'). \
...     option('sep', '\t'). \
...     schema('''employee_id INT, 
...       first_name STRING, 
...       last_name STRING, 
...       email STRING,
...       phone_number STRING, 
...       hire_date STRING, 
...       job_id STRING, 
...       salary FLOAT,
...       commission_pct STRING,
...       manager_id STRING, 
...       department_id STRING
...     '''). \
...     load(employeesPath)

    >>> employees.select('employee_id', 'department_id', 'salary').show()
    +-----------+-------------+-------+     
    |employee_id|department_id| salary|
    +-----------+-------------+-------+
    |127        |           50| 2400.0|
    |128        |           50| 2200.0|
    |129        |           50| 3300.0|
    |130        |           50| 2800.0|
    |131        |           50| 2500.0|
    |132        |           50| 2100.0|
    |133        |           50| 3300.0|
    |134        |           50| 2900.0|
    |135        |           50| 2400.0|
    |136        |           50| 2200.0|
    |137        |           50| 3600.0|
    |138        |           50| 3200.0|
    |139        |           50| 2700.0|
    |140        |           50| 2500.0|
    |141        |           50| 3500.0|
    |142        |           50| 3100.0|
    |143        |           50| 2600.0|
    |144        |           50| 2500.0|
    |145        |           80|14000.0|
    |146        |           80|13500.0|
    +-----------+-------------+-------+
    only showing top 20 rows
```
**Exercise 1**
Get salary by department
```python
    >>> employees.select('employee_id', 'department_id', 'salary').groupBy('department_id'). \
    ...     sum('salary').show()
    +-------------+-----------+
    |department_id|sum(salary)|
    +-------------+-----------+
    |           30|    24900.0|
    |           110|    20300.0|
    |           100|    51600.0|
    |            70|    10000.0|
    |            90|    58000.0|
    |            60|    28800.0|
    |            40|     6500.0|
    |            20|    19000.0|
    |            10|     4400.0|
    |            80|   304500.0|
    |           null|     7000.0|
    |           50|   156400.0|
    +-------------+-----------+
```

We will do the same with the agg function

```python
    from pyspark.sql.functions import sum
    >>> employees.select('employee_id', 'department_id', 'salary').groupBy('department_id'). \
    ... agg(sum(employees.salary).alias('salary_expense')).show()
    +-------------+--------------+
    |department_id|salary_expense|
    +-------------+--------------+
    |           30|         24900.0|
    |          110|         20300.0|
    |          100|         51600.0|
    |          70|          10000.0|
    |          90|          58000.0|
    |          60|          28800.0|
    |          40|           6500.0|
    |           20|         19000.0|
    |           10|          4400.0|
    |           80|        304500.0|
    |           null|        7000.0|
    |           50|         156400.0|
    +-------------+--------------+
```
However, if we want to compare a salary of an exployee_id in comparison with its deparment salary and understand which is the percentage salary of that employee, we have to self-join with exployees and take a few steps further.
In addition,we can use a windowing function to simplify the solution and make it much more efficient.

We will read orderItems:
```python
    orderItems = spark.read. \
    format('csv'). \
    schema('order_item_id int, order_item_order_id int, order_item_product_id int,order_item_quantity float, order_item_subtotal float,order_item_product_price float'). \
    load('/public/retail_db/order_items')
```
    This will create a window object
```python    
    >>> from pyspark.sql.window import * 
    >>> Window.partitionBy(orderItems.order_item_order_id)
    <pyspark.sql.window.WindowSpec object at 0x7fee0b146a10>

    spec = Window.partitionBy(orderItems.order_item_order_id)
    >>> orderItems.show()
    +-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
    |order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
    +-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
    |           1|                  1|                  957|                1.0|                299.98|                 299.98|
    |           2|                  2|                  1073|               1.0|                199.99|                 199.99|
    |           3|                  2|                  502|                5.0|                250.0|                   50.0|
    |           4|                  2|                  403|                1.0|                129.99|                 129.99|
    |           5|                  4|                  897|                2.0|                49.98|                   24.99|
    |           6|                  4|                  365|                5.0|                299.95|                  59.99|
    |           7|                  4|                  502|                3.0|                 150.0|                   50.0|
    |           8|                  4|                  1014|               4.0|                199.92|                  49.98|
    |           9|                  5|                  957|                1.0|                299.98|                 299.98|
    |           10|                 5|                  365|                5.0|                299.95|                  59.99|
    |           11|                 5|                  1014|               2.0|                 99.96|                  49.98|
    |           12|                 5|                  957|                1.0|                299.98|                 299.98|
    |           13|                 5|                  403|                1.0|                129.99|                 129.99|
    |           14|                 7|                  1073|               1.0|                199.99|                 199.99|
    |           15|                 7|                  957|                1.0|                299.98|                 299.98|
    |           16|                 7|                  926|                5.0|                 79.95|                  15.99|
    |           17|                 8|                  365|                3.0|                179.97|                  59.99|
    |           18|                 8|                  365|                5.0|                299.95|                  59.99|
    |           19|                 8|                  1014|               4.0|                199.92|                  49.98|
    |           20|                 8|                  502|                1.0|                  50.0|                   50.0|
    +-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
    only showing top 20 rows
```
Say we would like that information as well as the revenue generated by each order.    

```python
    >>> orderItems.\
    ...     withColumn('order_revenue', sum('order_item_subtotal').over(spec)).\
    ...     show()
    +-------------+-------------------+---------------------+-------------------+-------------------+------------------------+------------------+
    |order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|     order_revenue|
    +-------------+-------------------+---------------------+-------------------+-------------------+------------------------+------------------+
    |  348      |                   148|                502|                2.0|                100.0|                  50.0|4 79.99000549316406|
    |  349      |                   148|                502|                5.0|                250.0|                  50.0|479.99000549316406|
    |  350      |                   148|                403|                1.0|                129.99|                 129.99|479.99000549316406|
    | 1129      |                   463|                365|                4.0|                239.96|                 59.99| 829.9200096130371|
    | 1130      |                   463|                502|                5.0|                250.0|                  50.0|   829.9200096130371|
    | 1131      |                   463|                627|                1.0|                39.99|                  9.99| 829.9200096130371|
    | 1132      |                   463|                191|                3.0|                299.97|                 99.99| 829.9200096130371|
    | 1153      |                   471|                627|                1.0|                39.99|                  39.99|169.98000717163086|
    | 1154      |                   471|                403|                1.0|                129.99|                 129.99|169.98000717163086|
    | 1223      |                   496|                365|                1.0|                59.99|                  59.99|  441.950008392334|
    | 1224      |                   496|                502|                3.0|                150.0|                  50.0|  441.950008392334|
    | 1225      |                   496|                821|                1.0|                51.99|                  51.99|  441.950008392334|
    | 1226      |                   496|                403|                1.0|                129.99|                 129.99|  441.950008392334|
    | 1227      |                   496|                1014|               1.0|                49.98|                  49.98|  441.950008392334|
    | 2703      |                   1088|               403|                1.0|                129.99|                 129.99|249.97000885009766|
    | 2704      |                   1088|               365|                2.0|                119.98|                 59.99|249.97000885009766|
    | 3944      |                   1580|               44|                 5.0|                299.95|                 59.99|299.95001220703125|
    | 3968      |                   1591|               627|                5.0|                199.95|                 39.99| 439.8599967956543|
    | 3969      |                   1591|               627|                1.0|                39.99|                  39.99| 439.8599967956543|
    | 3970      |                   1591|               1014|               4.0|                199.92|                 49.98| 439.8599967956543|
    +-------------+-------------------+---------------------+-------------------+-------------------+------------------------+------------------+
    only showing top 20 rows
```
Now, if we want to round the order_revenue column

```python
    >>> from pyspark.sql.functions import round
    >>> orderItems.\
    ... withColumn('order_revenue', round(sum('order_item_subtotal').over(spec),2)).\
    ... select('order_item_id', 'order_item_order_id', 'order_item_subtotal', 'order_revenue').\
    ... show()
    +-------------+-------------------+-------------------+-------------+   
    |order_item_id|order_item_order_id|order_item_subtotal|order_revenue|
    +-------------+-------------------+-------------------+-------------+
    |  348      |                   148|            100.0|       479.99|
    |  349      |                   148|            250.0|       479.99|
    |  350      |                   148|            129.99|       479.99|
    | 1129      |                   463|            239.96|       829.92|
    | 1130      |                   463|            250.0|       829.92|
    | 1131      |                   463|             39.99|       829.92|
    | 1132      |                   463|            299.97|       829.92|
    | 1153      |                   471|             39.99|       169.98|
    | 1154      |                   471|            129.99|       169.98|
    | 1223      |                   496|             59.99|       441.95|
    | 1224      |                   496|            150.0|       441.95|
    | 1225      |                   496|            51.99|       441.95|
    | 1226      |                   496|            129.99|       441.95|
    | 1227      |                   496|            49.98|       441.95|
    | 2703      |                   1088|           129.99|          249.97|
    | 2704      |                   1088|           119.98|          249.97|
    | 3944      |                   1580|           299.95|          299.95|
    | 3968      |                   1591|           199.95|          439.86|
    | 3969      |                   1591|            39.99|          439.86|
    | 3970      |                   1591|           199.92|         439.86|   
    +-------------+-------------------+-------------------+-------------+
    only showing top 20 rows
```
**Exercise**
Get top N products per day

This is nothing but a ranking use case, so we will use the rank function which comes as part of the Window. 

```python
    orders = spark.read.format('csv').\
    schema('order_id int, order_date string, order_customer_id int,order_status string').\
    load('/public/retail_db/orders')
```    

We implement the daily revenue sorted: 


```python
from pyspark.sql.functions import sum, round    
dailyRevenue = orders.where('order_status in ("COMPLETE", "CLOSED")').\
 join(orderItems, orders.order_id == orderItems.order_item_order_id).\
 groupBy(orders.order_date, orderItems.order_item_product_id).\
 agg(round(sum(orderItems.order_item_subtotal),2).alias('revenue'))
```

dailyRevenueSorted = dailyRevenue.sort('order_date', dailyRevenue.revenue.desc())

    >>> dailyRevenueSorted.show(100)
    +--------------------+---------------------+--------+
    |  Order_date        |order_item_product_id| revenue|
    +--------------------+---------------------+--------+
    |2013-07-25 00:00:...|                  1004| 5599.72|
    |2013-07-25 00:00:...|                  191| 5099.49|
    |2013-07-25 00:00:...|                  957|  4499.7|
    |2013-07-25 00:00:...|                  365| 3359.44|
    |2013-07-25 00:00:...|                  1073| 2999.85|
    |2013-07-25 00:00:...|                  1014| 2798.88|
    |2013-07-25 00:00:...|                  403| 1949.85|
    |2013-07-25 00:00:...|                      502|  1650.0|
    |2013-07-25 00:00:...|                  627| 1079.73|
    |2013-07-25 00:00:...|                  226|  599.99|
    |2013-07-25 00:00:...|                   24|  319.96|
    |2013-07-25 00:00:...|                  821|  207.96|
    |2013-07-25 00:00:...|                  625|  199.99|
    |2013-07-25 00:00:...|                  705|  119.99|
    |2013-07-25 00:00:...|                  572|  119.97|
    |2013-07-25 00:00:...|                  666|  109.99|
    |2013-07-25 00:00:...|                  725|   108.0|
    |2013-07-25 00:00:...|                  134|   100.0|
    |2013-07-25 00:00:...|                  906|   99.96|


As we can see, for each product in a date, we have its revenue. 
What we want to have just the top N rows for each date 
For that reason, we have to use "order_date" in the partition function, and then we have to sort the data by revenue in descending order by revenue by date and then, we will create a rank
To understand the solution to the problem we will focus on some of the windowing functions available:
As for the Window function, we can use orderBy or partitionBy. When we do not have to order the data within each group, we will use partitionBy. However, for ranking purposes we will have to use partitionBy as well as orderBy.

```python
    spec = Window.partitionBy('order_date')
```
When we use this "spec" as part of "order" using aggregate or ranking functions, the dataframe in use should have a field called "order_date".
In our case, we have two datasets: orders and orderItems:
```python
    >>> orders.printSchema()
    root
     |-- order_id: integer (nullable = true)
     |-- order_date: string (nullable = true)
     |-- order_customer_id: integer (nullable = true)
     |-- order_status: string (nullable = true)

    >>> orderItems.printSchema()
    root
     |-- order_item_id: integer (nullable = true)
     |-- order_item_order_id: integer (nullable = true)
     |-- order_item_product_id: integer (nullable = true)
     |-- order_item_quantity: float (nullable = true)
     |-- order_item_subtotal: float (nullable = true)
     |-- order_item_product_price: float (nullable = true)
```
As orders dataframe is the only one having a field called "order_date", we can just use this dataframe for operations.

```python
    from pyspark.sql.functions import count
    orders.select('order_id', 'order_date',
    'order_customer_id',
    'order_status',
    count('order_date').over(spec).alias('daily_count')
    ).show()

    +--------+--------------------+-----------------+---------------+-----------+   
    |order_id|  Order_date      |order_customer_id|   order_status|daily_count|
    +--------+--------------------+-----------------+---------------+-----------+
    |    3378|2013-08-13 00:00:|     3155       |       PROCESSING|         73|
    |    3379|2013-08-13 00:00:|     5437       |       COMPLETE|           73|
    |    3380|2013-08-13 00:00:|     3519       |           CLOSED|         73|
    |    3381|2013-08-13 00:00:|    10023       |           ON_HOLD|        73|
    |    3382|2013-08-13 00:00:|     7856       |           PENDING|        73|
    |    3383|2013-08-13 00:00:|     1523       |   PENDING_PAYMENT|        73|
    |    3384|2013-08-13 00:00:|    12398       |   PENDING_PAYMENT|        73|
    |    3385|2013-08-13 00:00:|      132       |       COMPLETE|           73|
    |    3386|2013-08-13 00:00:|     2128       |       PENDING|            73|
    |    3387|2013-08-13 00:00:|     2735       |       COMPLETE|           73|
    |    3388|2013-08-13 00:00:|    12319       |       COMPLETE|           73|
    |    3389|2013-08-13 00:00:|     7024       |       COMPLETE|           73|
    |    3390|2013-08-13 00:00:|     5012       |       CLOSED|             73|
    |    3391|2013-08-13 00:00:|    11076       |     PROCESSING|           73|
    |    3392|2013-08-13 00:00:|     8989       |       CLOSED|             73|
    |    3393|2013-08-13 00:00:|    11247       |       COMPLETE|           73|
    |    3394|2013-08-13 00:00:|      890       |PENDING_PAYMENT|           73|
    |    3395|2013-08-13 00:00:|     1618       |       PENDING|            73|
    |    3396|2013-08-13 00:00:|     9491       |PENDING_PAYMENT|           73|
    |    3397|2013-08-13 00:00:|     6722       |PENDING_PAYMENT|           73|
    +--------+--------------------+-----------------+---------------+-----------+
    only showing top 20 rows
```

Normally we use:

- partitionBy for aggregations
- partitionBy + orderBy for aggregations



###Dataframe operations - Performing aggregations using sum, avg, etc

```python
from pyspark.sql import SparkSession

spark = SparkSession. \
builder. \
config('spark.ui.port', '0'). \
appName('Windowing Functions'). \
master('yarn'). \
getOrCreate()


employeesPath = '/public/hr_db/employees'

employees = spark. \
 read. \
 format('csv'). \
 option('sep', '\t'). \
 schema('''employee_id INT, 
   first_name STRING, 
   last_name STRING, 
   email STRING,
   phone_number STRING, 
   hire_date STRING, 
   job_id STRING, 
   salary FLOAT,
   commission_pct STRING,
   manager_id STRING, 
   department_id STRING
 '''). \
 load(employeesPath)    
```

If we print the schema of the dataframe created, we can see:
```python
    >>> employees.printSchema()
    root
     |-- employee_id: integer (nullable = true)
     |-- first_name: string (nullable = true)
     |-- last_name: string (nullable = true)
     |-- email: string (nullable = true)
     |-- phone_number: string (nullable = true)
     |-- hire_date: string (nullable = true)
     |-- job_id: string (nullable = true)
     |-- salary: float (nullable = true)
     |-- commission_pct: string (nullable = true)
     |-- manager_id: string (nullable = true)
     |-- department_id: string (nullable = true)
```
we see that manager_id and department_id fields are both string type. The reason being is because they, both, can have null values.
Out of all those fields, we are interested in salary, which is the field that we are goin to perform aggregations to, and in addition, we want to have employee_id and department_id.

```python
     >>> employees.select('employee_id', 'department_id', 'salary').show()
    +-----------+-------------+-------+
    |employee_id|department_id| salary|
    +-----------+-------------+-------+
    |       127|            50| 2400.0|
    |       128|            50| 2200.0|
    |       129|            50| 3300.0|
    |       130|            50| 2800.0|
    |       131|            50| 2500.0|
    |       132|            50| 2100.0|
    |       133|            50| 3300.0|
    |       134|            50| 2900.0|
    |       135|            50| 2400.0|
    |       136|            50| 2200.0|
    |       137|            50| 3600.0|
    |       138|            50| 3200.0|
    |       139|            50| 2700.0|
    |       140|            50| 2500.0|
    |       141|            50| 3500.0|
    |       142|            50| 3100.0|
    |       143|            50| 2600.0|
    |       144|            50| 2500.0|
    |       145|            80|14000.0|
    |       146|            80|13500.0|
    +-----------+-------------+-------+
    only showing top 20 rows
```

Now, say we want to have the salary comparing with the sum of the salaries of the department it belongs to:

```python
    >>> from pyspark.sql.window import * 
    >>> spec = Window.partitionBy('department_id')

    >>> employees.select('employee_id', 'department_id', 'salary').   \
    ... withColumn('salary_expense', sum('salary').over(spec)).\
    ... show()
    +-----------+-------------+-------+--------------+
    |employee_id|department_id| salary|salary_expense|
    +-----------+-------------+-------+--------------+
    |       114|            30|11000.0|       24900.0|
    |       115|            30| 3100.0|       24900.0|
    |       116|            30| 2900.0|       24900.0|
    |       117|            30| 2800.0|       24900.0|
    |       118|            30| 2600.0|       24900.0|
    |       119|            30| 2500.0|       24900.0|
    |       205|            110|12000.0|       20300.0|
    |       206|            110| 8300.0|       20300.0|
    |       108|            100|12000.0|       51600.0|
    |       109|            100| 9000.0|       51600.0|
    |       110|            100| 8200.0|       51600.0|
    |       111|            100| 7700.0|       51600.0|
    |       112|            100| 7800.0|       51600.0|
    |       113|            100| 6900.0|       51600.0|
    |       204|            70|10000.0|       10000.0|
    |       103|            60| 9000.0|       28800.0|
    |       104|            60| 6000.0|       28800.0|
    |       105|            60| 4800.0|       28800.0|
    |       106|            60| 4800.0|       28800.0|
    |       107|            60| 4200.0|       28800.0|
    +-----------+-------------+-------+--------------+
    only showing top 20 rows
```
**Explanation**
For department 70, as there is just one employee "salary_expense" will be equal to the salary of that employee.
Say we want to add the least salary paid within each department:
```python
    >>> employees.select('employee_id', 'department_id', 'salary').\
    ... withColumn('salary_expense', sum('salary').over(spec)).\
    ... withColumn('least_salary', min('salary').over(spec)).\
    ... show()
    +-----------+-------------+-------+--------------+------------+
    |employee_id|department_id| salary|salary_expense|least_salary|
    +-----------+-------------+-------+--------------+------------+
    |114        |   30      |11000.0|       24900.0|      2500.0|
    |115        |   30      | 3100.0|       24900.0|      2500.0|
    |116        |   30      | 2900.0|       24900.0|      2500.0|
    |117        |   30      | 2800.0|       24900.0|      2500.0|
    |118        |   30      | 2600.0|       24900.0|      2500.0|
    |119        |   30      | 2500.0|       24900.0|      2500.0|
    |205        |  110      |12000.0|       20300.0|      8300.0|
    |206        |  110      | 8300.0|       20300.0|      8300.0|
    |108        |  100      |12000.0|       51600.0|      6900.0|
    |109        |  100      | 9000.0|       51600.0|      6900.0|
    |110        |  100      | 8200.0|       51600.0|      6900.0|
    |111        |  100      | 7700.0|       51600.0|      6900.0|
    |112        |  100      | 7800.0|       51600.0|      6900.0|
    |113        |  100      | 6900.0|       51600.0|      6900.0|
    |204        |   70      |10000.0|       10000.0|     10000.0|
    |103        |   60      | 9000.0|       28800.0|      4200.0|
    |104        |   60      | 6000.0|       28800.0|      4200.0|
    |105        |   60      | 4800.0|       28800.0|      4200.0|
    |106        |   60      | 4800.0|       28800.0|      4200.0|
    |107        |   60      | 4200.0|       28800.0|      4200.0|
    +-----------+-------------+-------+--------------+------------+
    only showing top 20 rows
```

We add the max salary, the average salary and sort by department.
```python
    >>> employees.select('employee_id', 'department_id', 'salary').\
    ... withColumn('salary_expense', sum('salary').over(spec)).\
    ... withColumn('least_salary', min('salary').over(spec)).\
    ... withColumn('highest_salary', max('salary').over(spec)).\
    ... withColumn('average_salary', avg('salary').over(spec)).\
    ... show()
    +-----------+-------------+-------+--------------+------------+--------------+--------------+
    |employee_id|department_id| salary|salary_expense|least_salary|highest_salary|average_salary|
    +-----------+-------------+-------+--------------+------------+--------------+--------------+
    |114        |           30|11000.0|       24900.0|      2500.0|       11000.0|      4150.0|
    |115        |           30| 3100.0|       24900.0|      2500.0|       11000.0|      4150.0|
    |116        |           30| 2900.0|       24900.0|      2500.0|       11000.0|      4150.0|
    |117        |           30| 2800.0|       24900.0|      2500.0|       11000.0|      4150.0|
    |118        |           30| 2600.0|       24900.0|      2500.0|       11000.0|      4150.0|
    |119        |           30| 2500.0|       24900.0|      2500.0|       11000.0|      4150.0|
    |205        |           110|12000.0|       20300.0|      8300.0|       12000.0|       10150.0|
    |206        |           110| 8300.0|       20300.0|      8300.0|       12000.0|       10150.0|
    |108        |           100|12000.0|       51600.0|      6900.0|       12000.0|     8600.0|
    |109        |           100| 9000.0|       51600.0|      6900.0|       12000.0|     8600.0|
    |110        |           100| 8200.0|       51600.0|      6900.0|       12000.0|     8600.0|
    |111        |           100| 7700.0|       51600.0|      6900.0|       12000.0|     8600.0|
    |112        |           100| 7800.0|       51600.0|      6900.0|       12000.0|     8600.0|
    |113        |           100| 6900.0|       51600.0|      6900.0|       12000.0|     8600.0|
    |204        |           70|10000.0|       10000.0|     10000.0|       10000.0|       10000.0|
    |103        |           60| 9000.0|       28800.0|      4200.0|         9000.0|     5760.0|
    |104        |           60| 6000.0|       28800.0|      4200.0|         9000.0|     5760.0|
    |105        |           60| 4800.0|       28800.0|      4200.0|         9000.0|     5760.0|
    |106        |           60| 4800.0|       28800.0|      4200.0|         9000.0|     5760.0|
    |107        |           60| 4200.0|       28800.0|      4200.0|         9000.0|     5760.0|
    +-----------+-------------+-------+--------------+------------+--------------+--------------+
    only showing top 20 rows
```


If we want to calculate each salary percentage against total salary of each department:
- we have to import the col function because earlier we mentioned that salary_expense was defined as a string because we have to manage null possible values
```python
    >>> from pyspark.sql.functions import col
    >>> employees.select('employee_id', 'department_id', 'salary').\
    ... withColumn('salary_expense', sum('salary').over(spec)).\
    ... withColumn('salary_pct', employees.salary/col('salary_expense')).\
    ... show()
    +-----------+-------------+-------+--------------+-------------------+
    |employee_id|department_id| salary|salary_expense| salary_pct|
    +-----------+-------------+-------+--------------+-------------------+
    |       114|    30      |11000.0|       24900.0|0.44176706827309237|
    |       115|   30       | 3100.0|       24900.0|0.12449799196787148|
    |       116|   30       | 2900.0|       24900.0|0.11646586345381527|
    |       117|   30       | 2800.0|       24900.0|0.11244979919678715|
    |       118|   30       | 2600.0|       24900.0|0.10441767068273092|
    |       119|   30       | 2500.0|       24900.0|0.10040160642570281|
    |       205|  110       |12000.0|       20300.0| 0.5911330049261084|
    |       206|  110       | 8300.0|       20300.0| 0.4088669950738916|
    |       108|  100       |12000.0|       51600.0|0.23255813953488372|
    |       109|  100       | 9000.0|       51600.0| 0.1744186046511628|
    |       110|  100       | 8200.0|       51600.0|0.15891472868217055|
    |       111|  100       | 7700.0|       51600.0|0.14922480620155038|
    |       112|  100       | 7800.0|       51600.0| 0.1511627906976744|
    |       113|  100       | 6900.0|       51600.0|0.13372093023255813|
    |       204|   70       |10000.0|       10000.0|                1.0|
    |       103|   60       | 9000.0|       28800.0|            0.3125|
    |       104|   60       | 6000.0|       28800.0|0.20833333333333334|
    |       105|   60       | 4800.0|       28800.0|0.16666666666666666|
    |       106|   60       | 4800.0|       28800.0|0.16666666666666666|
    |       107|   60       | 4200.0|       28800.0|0.14583333333333334|
    +-----------+-------------+-------+--------------+-------------------+
    only showing top 20 rows



    >>> employees.select('employee_id', 'department_id', 'salary').\
    ...  withColumn('salary_expense', sum('salary').over(spec)).\
    ...  withColumn('salary_pct',round(( employees.salary/col('salary_expense'))*100,2)).\
    ... show()

    +-----------+-------------+-------+--------------+----------+
    |employee_id|department_id| salary|salary_expense|salary_pct|
    +-----------+-------------+-------+--------------+----------+
    |114        |           30|11000.0|       24900.0|     44.18|
    |115        |           30| 3100.0|       24900.0|     12.45|
    |116        |           30| 2900.0|       24900.0|     11.65|
    |117        |           30| 2800.0|       24900.0|     11.24|
    |118        |           30| 2600.0|       24900.0|     10.44|
    |119        |           30| 2500.0|       24900.0|     10.04|
    |205        |           110|12000.0|       20300.0|     59.11|
    |206        |           110| 8300.0|       20300.0|     40.89|
    |108        |           100|12000.0|       51600.0|     23.26|
    |109        |           100| 9000.0|       51600.0|     17.44|
    |110        |           100| 8200.0|       51600.0|     15.89|
    |111        |           100| 7700.0|       51600.0|     14.92|
    |112        |           100| 7800.0|       51600.0|     15.12|
    |113        |           100| 6900.0|       51600.0|     13.37|
    |204        |            70|10000.0|       10000.0|     100.0|
    |103        |            60| 9000.0|       28800.0|     31.25|
    |104        |           60| 6000.0|       28800.0|     20.83|
    |105        |           60| 4800.0|       28800.0|     16.67|
    |106        |           60| 4800.0|       28800.0|     16.67|
    |107        |           60| 4200.0|       28800.0|     14.58|
    +-----------+-------------+-------+--------------+----------+
    only showing top 20 rows
```



###Dataframe operations: time series functions such us Lead, Lag, etc
Let us see details about windowing functions where data is partitioned by a key (such as department) and then sorted by some other key (such as hire date)
- We have functions such as lead, lag, first, last, etc.

For most of those functions we have to use partitionBy and then, orderBy some other key.
Those 4 functions can take any field within the group. They might not select the same fields which were taken as part of the orderBy.

**Example**
```python
    import pyspark
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('Exploring windowing functions').master('local').getOrCreate()
    employeesPath = '/public/hr_db/employees'
    employees = spark. \
 read. \
 format('csv'). \
 option('sep', '\t'). \
 schema('''employee_id INT, 
   first_name STRING, 
   last_name STRING, 
   email STRING,
   phone_number STRING, 
   hire_date STRING, 
   job_id STRING, 
   salary FLOAT,
   commission_pct STRING,
   manager_id STRING, 
   department_id STRING
 '''). \
 load(employeesPath)

    >>> employees.show()
    +-----------+----------+----------+--------+------------------+----------+--------+-------+--------------+----------+-------------+
    |employee_id|first_name| last_name|   email|      phone_number| hire_date|  job_id| salary|commission_pct|manager_id|department_id|
    +-----------+----------+----------+--------+------------------+----------+--------+-------+--------------+----------+-------------+
    |127        |     James|    Landry| JLANDRY|      650.124.1334|1999-01-14|ST_CLERK| 2400.0|         null|       120|   50|
    |128        |    Steven|    Markle| SMARKLE|      650.124.1434|2000-03-08|ST_CLERK| 2200.0|         null|       120|   50|
    |129        |     Laura|    Bissot| LBISSOT|      650.124.5234|1997-08-20|ST_CLERK| 3300.0|         null|       121|   50|
    |130        |     Mozhe|  Atkinson|MATKINSO|      650.124.6234|1997-10-30|ST_CLERK| 2800.0|         null|       121|   50|
    |131        |     James|    Marlow| JAMRLOW|      650.124.7234|1997-02-16|ST_CLERK| 2500.0|         null|       121|   50|
    |132        |       TJ|     Olson| TJOLSON|      650.124.8234|1999-04-10|ST_CLERK| 2100.0|          null|       121|   50|
    |133        |     Jason|    Mallin| JMALLIN|      650.127.1934|1996-06-14|ST_CLERK| 3300.0|         null|       122|   50|
    |134        |   Michael|    Rogers| MROGERS|      650.127.1834|1998-08-26|ST_CLERK| 2900.0|         null|       122|   50|
    |135        |       Ki|       Gee|    KGEE|      650.127.1734|1999-12-12|ST_CLERK|  2400.0|         null|       122|   50|
    |136        |     Hazel|Philtanker|HPHILTAN|      650.127.1634|2000-02-06|ST_CLERK| 2200.0|         null|       122|   50|
    |137        |    Renske|    Ladwig| RLADWIG|      650.121.1234|1995-07-14|ST_CLERK| 3600.0|         null|       123|   50|
    |138        |   Stephen|    Stiles| SSTILES|      650.121.2034|1997-10-26|ST_CLERK| 3200.0|         null|       123|   50|
    |139        |      John|       Seo|    JSEO|      650.121.2019|1998-02-12|ST_CLERK| 2700.0|         null|       123|   50|
    |140        |    Joshua|     Patel|  JPATEL|      650.121.1834|1998-04-06|ST_CLERK| 2500.0|         null|       123|   50|
    |141        |    Trenna|      Rajs|   TRAJS|      650.121.8009|1995-10-17|ST_CLERK| 3500.0|         null|       124|   50|
    |142        |    Curtis|    Davies| CDAVIES|      650.121.2994|1997-01-29|ST_CLERK| 3100.0|         null|       124|   50|
    |143        |   Randall|     Matos|  RMATOS|      650.121.2874|1998-03-15|ST_CLERK| 2600.0|         null|       124|   50|
    |144        |     Peter|    Vargas| PVARGAS|      650.121.2004|1998-07-09|ST_CLERK| 2500.0|         null|       124|   50|
    |145        |      John|   Russell| JRUSSEL|011.44.1344.429268|1996-10-01|  SA_MAN|14000.0|         0.40|       100|   80|
    |146        |     Karen|  Partners|KPARTNER|011.44.1344.467268|1997-01-05|  SA_MAN|13500.0|         0.30|       100|   80|
    +-----------+----------+----------+--------+------------------+----------+--------+-------+--------------+----------+-------------+
    only showing top 20 rows
    
    spark.sonf.set('sparlk.sql.shuffle.partitions','2')  ----> to make it partition data in just two parts
```
We are going to use just three fields from exployees:
```
    >>> employees.select('employee_id','department_id', 'salary').sort('department_id').show()
    +-----------+-------------+-------+
    |employee_id|department_id| salary|
    +-----------+-------------+-------+
    |200        |           10| 4400.0|
    |111        |           100| 7700.0|
    |109        |           100| 9000.0|
    |112        |           100| 7800.0|
    |108        |           100|12000.0|
    |113        |           100| 6900.0|
    |110        |           100| 8200.0|
    |205        |           110|12000.0|
    |206        |           110| 8300.0|
    |201        |            20|13000.0|
    |202        |           20| 6000.0|
    |115        |           30| 3100.0|
    |119        |           30| 2500.0|
    |114        |           30|11000.0|
    |118        |           30| 2600.0|
    |116        |           30| 2900.0|
    |117        |           30| 2800.0|
    |203        |           40| 6500.0|
    |186        |           50| 3400.0|
    |187        |           50| 3000.0|
    +-----------+-------------+-------+
    only showing top 20 rows
```

Considering these rows:
```python
    +-----------+-------------+-------+
    |employee_id|department_id| salary|
    +-----------+-------------+-------+
    |       111|            100| 7700.0|
    |       109|            100| 9000.0|
    |       112|            100| 7800.0|
    |       108|            100|12000.0|
    |       113|            100| 6900.0|
    |       110|            100| 8200.0|

```
**Exercise**
--------
we want to know the highest paid employee, the second highest paid and the difference between current salary and the next best salary paid employee. 

    - Lag: if we want to get previous element based on a criteria
    - Lead: if we want to have the next element based on a criteria.

```python
from pyspark.sql.window import *
spec = window.\
... partitionBy('department_id'). \  ---> we are trying to get the next better paid within each department
    orderBy(employees.salary.desc()) ---> we want to order employees based on salary

from pyspark.sql.functions import lead
>>> employees.select('employee_id','department_id', 'salary').\
    withColumn('next_employee_id'), lead('employee_id').over(spec)).\---> we need to add some more columns (as part of lead we add)
  'employee_id' since we want to know who is the next better paid
    sort('department_id', employees.salary.desc()).\
    show()

    ... show()
    +-----------+-------------+-------+----------------+    
    |employee_id|department_id| salary|next_employee_id|
    +-----------+-------------+-------+----------------+
    |200        |   10      | 4400.0|               null|
    |108        |  100      |12000.0|               109|
    |109        |  100      | 9000.0|               110|
    |110        |  100      | 8200.0|               112|
    |112        |  100      | 7800.0|               111|
    |111        |  100      | 7700.0|               113|
    |113        |  100      | 6900.0|               null|
    |205        |  110      |12000.0|               206|
    |206        |  110      | 8300.0|               null|
    |201        |   20      |13000.0|                202|
    |202        |   20      | 6000.0|               null|
    |114        |   30      |11000.0|                115|
    |115        |   30      | 3100.0|               116|
    |116        |   30      | 2900.0|               117|
    |117        |   30      | 2800.0|               118|
    |118        |   30      | 2600.0|               119|
    |119        |   30      | 2500.0|               null|
    |203        |   40      | 6500.0|               null|
    |121        |   50      | 8200.0|               120|
    |120        |   50      | 8000.0|               122|
    +-----------+-------------+-------+----------------+
    only showing top 20 rows
```
Now, we want to compare current salary with the next best salary paid employee.
```python
    >>> employees.select ('employee_id', 'department_id','salary').\
    ... withColumn('next_employee_id', lead('employee_id').over(spec)).\
    ... withColumn('next_salary', lead('salary').over(spec)). \
    ... sort('department_id', employees.salary.desc()).\
    ... show()
    +-----------+-------------+-------+----------------+-----------+
    |employee_id|department_id| salary|next_employee_id|next_salary|
    +-----------+-------------+-------+----------------+-----------+
    |200        |   10      |    4400.0|    null|       null|
    |108        |  100      |   12000.0|     109|     9000.0|
    |109        |  100      |    9000.0|     110|     8200.0|
    |110        |  100      |    8200.0|     112|     7800.0|
    |112        |  100      |    7800.0|     111|     7700.0|

```

Now, if we want to get the difference between salaries:
```python
    >>> employees.select ('employee_id', 'department_id','salary').\
    ... withColumn('next_employee_id', lead('employee_id').over(spec)).\
    ... withColumn('next_salary', employees.salary - lead('salary').over(spec)). \
    ... sort('department_id', employees.salary.desc()).\
    ... show()
    +-----------+-------------+-------+----------------+-----------+
    |employee_id|department_id| salary|next_employee_id|next_salary|
    +-----------+-------------+-------+----------------+-----------+
    |200        |           10| 4400.0|             null|       null|
    |108        |           100|12000.0|             109|     3000.0|
    |109        |           100| 9000.0|            110|      800.0|
    |110        |           100| 8200.0|            112|      400.0|
    |112        |           100| 7800.0|            111|      100.0|
    |111        |           100| 7700.0|            113|      800.0|
    |113        |           100| 6900.0|            null|       null|
    |205        |           110|12000.0|             206|     3700.0|
    |206        |           110| 8300.0|            null|       null|
    |201        |            20|13000.0|            202|     7000.0|
    |202        |            20| 6000.0|            null|       null|
    |114        |            30|11000.0|             115|     7900.0|
    |115        |            30| 3100.0|            116|      200.0|
    |116        |           30| 2900.0|             117|      100.0|
    |117        |           30| 2800.0|             118|      200.0|
    |118        |           30| 2600.0|             119|      100.0|
    |119        |           30| 2500.0|             null|       null|
    |203        |           40| 6500.0|             null|       null|
    |121        |           50| 8200.0|             120|      200.0|
    |120        |           50| 8000.0|             122|      100.0|
    +-----------+-------------+-------+----------------+-----------+
    only showing top 20 rows
```
We can pass additional options to the lead function:
- lead(salary,1): we include next item details
- lead(salary,2): we include x2 next item details

```python
>>> employees.select ('employee_id', 'department_id','salary').\
... withColumn('next_employee_id', lead('employee_id',2).over(spec)).\
... withColumn('next_salary', lead('salary',2).over(spec)).\
... sort('department_id', employees.salary.desc()).\
... show()

+-----------+-------------+-------+----------------+-----------+
|employee_id|department_id| salary|next_employee_id|next_salary|
+-----------+-------------+-------+----------------+-----------+
|200        |           10| 4400.0|             null|       null|
|108        |           100|12000.0|            110|     8200.0|
|109        |           100| 9000.0|            112|     7800.0|
|110        |           100| 8200.0|            111|     7700.0|
|112        |           100| 7800.0|            113|     6900.0|
|111        |           100| 7700.0|            null|       null|
|113        |           100| 6900.0|            null|       null|
|205        |           110|12000.0|            null|       null|
|206        |           110| 8300.0|            null|       null|
|201        |           20|13000.0|             null|       null|
|202        |           20| 6000.0|             null|       null|
|114        |           30|11000.0|             116|     2900.0|
|115        |           30| 3100.0|             117|     2800.0|
|116        |           30| 2900.0|             118|     2600.0|
|117        |           30| 2800.0|             119|     2500.0|
|118        |           30| 2600.0|             ull|       null|
|119        |           30| 2500.0|             null|       null|
|203        |           40| 6500.0|             null|       null|
|121        |           50| 8200.0|             122|     7900.0|
|120        |           50| 8000.0|             123|     6500.0|
+-----------+-------------+-------+----------------+-----------+
only showing top 20 rows
```

Here, we are sayin in the column "next_employee_id" the second best paid salary and, in next_salary column, the amount that corresponds to that employee.

```
+-----------+-------------+-------+----------------+-----------+
|employee_id|department_id| salary|next_employee_id|next_salary|
+-----------+-------------+-------+----------------+-----------+
|108        |           100|12000.0|            110|     8200.0|
```

**Exercise**: get previous employee detail.

```python
>>> from pyspark.sql.functions import lag
>>> employeesLag = employees. \
...   select('employee_id', 'salary', 'department_id'). \
...   withColumn('lag_salary', lag(employees.salary, 1).over(spec)). \
...   orderBy(employees.department_id, employees.salary.desc())
>>> employeesLag.show(200)
+-----------+-------+-------------+----------+
|employee_id| salary|department_id|lag_salary|
+-----------+-------+-------------+----------+
|200        | 4400.0|           10|      null|
|108        |12000.0|           100|      null|
|109        | 9000.0|           100|   12000.0|
|110        | 8200.0|           100|    9000.0|
|112        | 7800.0|           100|    8200.0|
|111        | 7700.0|           100|    7800.0|
|113        | 6900.0|           100|    7700.0|
|205        |12000.0|           110|      null|
|206        | 8300.0|           110|   12000.0|
|201        |13000.0|           20|      null|
|202        | 6000.0|           20|   13000.0|
|114        |11000.0|           30|      null|
|115        | 3100.0|           30|   11000.0|
|116        | 2900.0|           30|    3100.0|
|117        | 2800.0|           30|    2900.0|
```
Here we show previous employee_id as well as his salary:

```python
employeesLag = employees. \
...   select('employee_id', 'salary', 'department_id'). \
...   withColumn('lag_employee_id', lag(employees.employee_id, 1).over(spec)). \
...   withColumn('lag_salary', lag(employees.salary, 1).over(spec)). \
...   orderBy(employees.department_id, employees.salary.desc())
>>> employeesLag.show(200)
+-----------+-------+-------------+---------------+----------+
|employee_id| salary|department_id|lag_employee_id|lag_salary|
+-----------+-------+-------------+---------------+----------+
|200        | 4400.0|           10|             null|      null|
|108        |12000.0|           100|            null|      null|
|109        | 9000.0|           100|            108|   12000.0|
|110        | 8200.0|           100|            109|    9000.0|
|112        | 7800.0|           100|            110|    8200.0|
|111        | 7700.0|           100|            112|    7800.0|
|113        | 6900.0|           100|            111|    7700.0|
|205        |12000.0|           110|            null|      null|
|206        | 8300.0|           110|            205|   12000.0|

```

**Example with the first function**
Get the best paid salary and the employee_id it belongs to within each department.

```python
>>> from pyspark.sql.functions import first
>>> 
>>> employeesFirst = employees. \
...   select('employee_id', 'salary', 'department_id'). \
...   withColumn('first_salary', first(employees.salary).over(spec)). \
...   orderBy(employees.department_id, employees.salary.desc())
>>> 
>>> employeesFirst.show(200)
+-----------+-------+-------------+------------+
|employee_id| salary|department_id|first_salary|
+-----------+-------+-------------+------------+
|200        | 4400.0|           10|      4400.0|
|108        |12000.0|           100|     12000.0|
|109        | 9000.0|           100|     12000.0|
|110        | 8200.0|           100|     12000.0|
|112        | 7800.0|           100|     12000.0|
|111        | 7700.0|           100|     12000.0|
|113        | 6900.0|           100|     12000.0|
|205        |12000.0|           110|     12000.0|
|206        | 8300.0|           110|     12000.0|
|201        |13000.0|            20|     13000.0|
|202        | 6000.0|            20|     13000.0|
|114        |11000.0|            30|     11000.0|
|115        | 3100.0|            30|     11000.0|
|116        | 2900.0|            30|     11000.0|
|117        | 2800.0|            30|     11000.0|
|118        | 2600.0|            30|     11000.0|
```

Example with the last function:
As we can see, if we just add last this way, it gets current employee_id and his salary, not the last employee_id and salary within that department.

```python
from pyspark.sql.functions import last
>>> employeesLast = employees. \
...   select('employee_id', 'salary', 'department_id'). \
...   withColumn('lasst_salary', last(employees.salary).over(spec)). \
...   orderBy(employees.department_id, employees.salary.desc())
>>> employeesLast.show()
+-----------+-------+-------------+------------+
|employee_id| salary|department_id|lasst_salary|
+-----------+-------+-------------+------------+
|200        | 4400.0|           10|      4400.0|
|108        |12000.0|           100|     12000.0|
|109        | 9000.0|           100|      9000.0|
|110        | 8200.0|           100|      8200.0|
|112        | 7800.0|           100|      7800.0|
|111        | 7700.0|           100|      7700.0|
|113        | 6900.0|           100|      6900.0|
|205        |12000.0|           110|     12000.0|
|206        | 8300.0|           110|      8300.0|
|201        |13000.0|            20|     13000.0|
|202        | 6000.0|            20|      6000.0|
|114        |11000.0|            30|     11000.0|
|115        | 3100.0|            30|      3100.0|
|116        | 2900.0|            30|      2900.0|
|117        | 2800.0|            30|      2800.0|
|118        | 2600.0|            30|      2600.0|
|119        | 2500.0|            30|      2500.0|
|203        | 6500.0|            40|      6500.0|
|121        | 8200.0|            50|      8200.0|
|120        | 8000.0|            50|      8000.0|
+-----------+-------+-------------+------------+
only showing top 20 rows
```

We must pass some additional fields to that function to correct it:

```python
>>> spec = Window. \
...   partitionBy('department_id'). \
...   orderBy(employees.salary.desc()). \
...   rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
>>> employeesLast = employees. \
...   select('employee_id', 'salary', 'department_id'). \
...   withColumn('last_salary', last(employees.salary, False).over(spec)). \   --->  we must pay attention to the "False" part
...   orderBy(employees.department_id, employees.salary.desc())
>>> employeesLast.show(200)
+-----------+-------+-------------+-----------+
|employee_id| salary|department_id|last_salary|
+-----------+-------+-------------+-----------+
|200        | 4400.0|           10|     4400.0|
|108        |12000.0|           100|     6900.0|
|109        | 9000.0|           100|     6900.0|
|110        | 8200.0|           100|     6900.0|
|112        | 7800.0|           100|     6900.0|
|111        | 7700.0|           100|     6900.0|
|113        | 6900.0|           100|     6900.0|
|205        |12000.0|           110|     8300.0|
|206        | 8300.0|           110|     8300.0|
|201        |13000.0|           20|     6000.0|
|202        | 6000.0|           20|     6000.0|
|114        |11000.0|           30|     2500.0|
|115        | 3100.0|           30|     2500.0|
|116        | 2900.0|           30|     2500.0|
|117        | 2800.0|           30|     2500.0|
|118        | 2600.0|           30|     2500.0|
|119        | 2500.0|           30|     2500.0|

```

###Ranking functions - rank,dense_rank,row_number, etc
The data normally is partitioned by a key (such as department) and then sorted by some other key (such us salary).

- We have functions like rank, dense_rank, row_number, etc
- We need to create a WindowSpec object using partitionBy and then orderBy for most of the ranking functions.

```python
    import pyspark
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('Exploring windowing functions').master('local').getOrCreate()
    employeesPath = '/public/hr_db/employees'
    employees = spark. \
 read. \
 format('csv'). \
 option('sep', '\t'). \
 schema('''employee_id INT, 
   first_name STRING, 
   last_name STRING, 
   email STRING,
   phone_number STRING, 
   hire_date STRING, 
   job_id STRING, 
   salary FLOAT,
   commission_pct STRING,
   manager_id STRING, 
   department_id STRING
 '''). \
 load(employeesPath)
```

Just to preview the data with the three fields we are interested in:

```python
 >>> employees. \
... select('employee_id', 'department_id', 'salary'). \
... orderBy(employees.department_id, employees.salary.desc()).\
... show()
+-----------+-------------+-------+
|employee_id|department_id| salary|
+-----------+-------------+-------+
|200        |           10| 4400.0|
|108        |           100|12000.0|
|109        |           100| 9000.0|
|110        |           100| 8200.0|
|112        |           100| 7800.0|
|111        |           100| 7700.0|
|113        |           100| 6900.0|
|205        |           110|12000.0|
|206        |           110| 8300.0|
|201        |           20|13000.0|
|202        |           20| 6000.0|
|114        |           30|11000.0|
|115        |           30| 3100.0|
|116        |           30| 2900.0|
|117        |           30| 2800.0|
|118        |           30| 2600.0|
|119        |           30| 2500.0|
|203        |           40| 6500.0|
|121        |           50| 8200.0|
|120        |           50| 8000.0|
+-----------+-------------+-------+
only showing top 20 rows
```
Now, I want to assign ranks based on that salary as criteria.

```python
>>> spec = Window. \
... partitionBy('department_id').\
... orderBy(employees.salary.desc())

>>> from pyspark.sql.functions import rank

>>> employees. \
... select('employee_id','department_id', 'salary'). \
... withColumn('rank', rank().over(spec)). \
... orderBy('department_id', employees.salary.desc()). \
... show()
+-----------+-------------+-------+----+
|employee_id|department_id| salary|rank|
+-----------+-------------+-------+----+
|200        |           10| 4400.0|   1|
|108        |           100|12000.0|   1|
|109        |           100| 9000.0|   2|
|110        |           100| 8200.0|   3|
|112        |           100| 7800.0|   4|
|111        |           100| 7700.0|   5|
|113        |           100| 6900.0|   6|
|205        |           110|12000.0|   1|
|206        |           110| 8300.0|   2|
|201        |           20|13000.0|   1|
|202        |           20| 6000.0|   2|
|114        |           30|11000.0|   1|
|115        |           30| 3100.0|   2|
|116        |           30| 2900.0|   3|
|117        |           30| 2800.0|   4|
|118        |           30| 2600.0|   5|
|119        |           30| 2500.0|   6|
|203        |           40| 6500.0|   1|
|121        |           50| 8200.0|   1|
|120        |           50| 8000.0|   2|
+-----------+-------------+-------+----+
only showing top 20 rows
```

The difference between rank and dense_rank is taht, if two employees are having the same salary in a department, they will be given the same rank position with the rank function:
With dense_rank, despite the fact of having the same salary, they would be given consecutive positions.

**Dense_rank**

```python
from pyspark.sql.functions import dense_rank

>>> employees. \
... select('employee_id','department_id', 'salary'). \
... withColumn('rank', rank().over(spec)). \
... withColumn('dense_rank', dense_rank().over(spec)). \
...  orderBy('department_id', employees.salary.desc()). \
... show()
+-----------+-------------+-------+----+----------+
|employee_id|department_id| salary|rank|dense_rank|
+-----------+-------------+-------+----+----------+
|200        |           10| 4400.0|   1|        1|
|108        |           100|12000.0|   1|       1|
|109        |           100| 9000.0|   2|       2|
|110        |           100| 8200.0|   3|       3|
|112        |           100| 7800.0|   4|       4|
|111        |           100| 7700.0|   5|       5|
|113        |           100| 6900.0|   6|       6|
|205        |           110|12000.0|   1|       1|
|206        |           110| 8300.0|   2|       2|
|201        |           20|13000.0|   1|        1|
|202        |           20| 6000.0|   2|        2|
|114        |           30|11000.0|   1|        1|
|115        |           30| 3100.0|   2|        2|
|116        |           30| 2900.0|   3|        3|
|117        |           30| 2800.0|   4|        4|
|118        |           30| 2600.0|   5|        5|
|119        |           30| 2500.0|   6|        6|
|203        |           40| 6500.0|   1|        1|
|121        |           50| 8200.0|   1|        1|
|120        |           50| 8000.0|   2|        2|
+-----------+-------------+-------+----+----------+
only showing top 20 rows
```

If we want to assign a row_number based on a criteria:
```python
>>> from pyspark.sql.functions import row_number
>>> employees. \
... select('employee_id','department_id', 'salary'). \
... withColumn('rank', rank().over(spec)). \
... withColumn('dense_rank', dense_rank().over(spec)). \
... withColumn('row_number', row_number().over(spec)). \
...  orderBy('department_id', employees.salary.desc()). \
... show()
+-----------+-------------+-------+----+----------+----------+
|employee_id|department_id| salary|rank|dense_rank|row_number|
+-----------+-------------+-------+----+----------+----------+
|200        |           10| 4400.0|   1|        1|          1|
|108        |           100|12000.0|   1|       1|          1|
|109        |           100| 9000.0|   2|       2|          2|
|110        |           100| 8200.0|   3|       3|          3|
|112        |           100| 7800.0|   4|       4|          4|
|111        |           100| 7700.0|   5|       5|          5|
|113        |           100| 6900.0|   6|       6|          6|
|205        |           110|12000.0|   1|       1|          1|
|206        |           110| 8300.0|   2|       2|          2|
|201        |           20|13000.0|   1|        1|          1|
|202        |           20| 6000.0|   2|        2|          2|
|114        |           30|11000.0|   1|        1|          1|
|115        |           30| 3100.0|   2|        2|          2|
|116        |           30| 2900.0|   3|        3|          3|
|117        |           30| 2800.0|   4|        4|          4|
|118        |           30| 2600.0|   5|        5|          5|
|119        |           30| 2500.0|   6|        6|          6|
|203        |           40| 6500.0|   1|        1|          1|
|121        |           50| 8200.0|   1|        1|          1|
|120        |           50| 8000.0|   2|        2|          2|
+-----------+-------------+-------+----+----------+----------+
only showing top 20 rows
>>> from pyspark.sql.functions import row_number
>>> employees. \
... select('employee_id','department_id', 'salary'). \
... withColumn('rank', rank().over(spec)). \
... withColumn('dense_rank', dense_rank().over(spec)). \
... withColumn('row_number', row_number().over(spec)). \
...  orderBy('department_id', employees.salary.desc()). \
... show()
+-----------+-------------+-------+----+----------+----------+
|employee_id|department_id| salary|rank|dense_rank|row_number|
+-----------+-------------+-------+----+----------+----------+
|200        |           10| 4400.0|   1|        1|          1|
|108        |           100|12000.0|   1|       1|          1|
|109        |           100| 9000.0|   2|       2|          2|
|110        |           100| 8200.0|   3|       3|          3|
|112        |           100| 7800.0|   4|       4|          4|
|111        |           100| 7700.0|   5|       5|          5|
|113        |           100| 6900.0|   6|       6|          6|
|205        |           110|12000.0|   1|       1|          1|
|206        |           110| 8300.0|   2|       2|          2|
|201        |            20|13000.0|   1|       1|          1|
|202        |           20| 6000.0|   2|        2|          2|
|114        |           30|11000.0|   1|        1|          1|
|115        |           30| 3100.0|   2|        2|          2|
|116        |           30| 2900.0|   3|        3|          3|
|117        |           30| 2800.0|   4|        4|          4|
|118        |           30| 2600.0|   5|        5|          5|
|119        |           30| 2500.0|   6|        6|          6|
|203        |           40| 6500.0|   1|        1|          1|
|121        |           50| 8200.0|   1|        1|          1|
|120        |           50| 8000.0|   2|        2|          2|
+-----------+-------------+-------+----+----------+----------+
only showing top 20 rows
```



- Some realistic use cases:
    - Assign rank to employees based on salary within each department.
    - Assign ranks to products based on revenue each day or month.

    To know how to manage the resources, we have to know the size of the file we will be working with, as well as the capicity of the cluster:

```python
    [carlos_sanchez@gw03 ~]$ hdfs dfs -ls -h /public/crime/csv
    Found 1 items
    -rw-r--r--   3 hdfs hdfs      1.4 G 2017-08-08 04:34 /public/crime/csv/crime_data.csv
```

 As part of the certification exam, you will be given the resource manager page or you gill be given the resource manager name (the port for the resource manager is always 8088):
```python
    rm01.itversity.com:19088
```

###Cluster Metrics
We have to take a look to the columns of "VCores Total" and "Memory Total". It means that the number of cores defined in "VCores Total" handle the capacity shown in "MemoryTotal" field.
The minimum allocation for any task will be given by the "Minimum allocation" column (one row below the others)  and "Maximum allocation".
In our case, minimum allocation is defined as <memory:1024, vCores:1> and maximum allocation <memory:4096, vCores:2>.
So, by default, it takes 1 core (what is shown in the "minimum allocation" variable) and 1024 MB RAM (1GB).
Number of cores = I have to multiply number of VCores to the number determined in "minimum allocation" parameter. That number should not be higher than the "Memory total paremeter"
The number of executors should be "VCores total" divided by the "minimum allocation"

We can customize the cluster by launching it this way:

```python
    pyspark --master yarn \
      --conf spark.ui.port=12345
      --num-executors 6 \
      --executor-cores 2 \
      --executor-memory 2G
```





