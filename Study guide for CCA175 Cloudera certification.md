Study guide for CCA175 Cloudera certification
=============================================

### Create a directory path
---------------

Create a directory path recursively
```console
 mkdir -p retail_app/src/main/python
```


### Launch spark shell
---------------

If we launch spark without defining the port, it will connect to port 4000 if it is not occupied

```console
 spark-shell --master yarn --conf spark.ui.port=12245
```
or, to let the cluster select a port randomly:
```console
pyspark --master yarn --conf spark.ui.port=0
```


### Know the size of a directory
---------------
```console
 du -sh /data/retail_db
```


### List of files in a directory in the cluster
---------------
```console
 hadoop fs -ls /public
```


### Know the size of a hadoop directory
---------------
```console
hadoop fs -du -s -h /public
```

## HDFS
The command line for hadoop is:
```console
hadoop fs 
```
or (they are both equivalent)

```
hdfs dfs
```

From gateway node, we can go to the location /etc/hadoop/conf
Here we can see properties files which control the environment of HDFS, YARN etc

```console
/etc/hadoop/conf/core-site.xml ---> information about the cluster
/etc/hadoop/conf/hdfs-site.xml ----> it contains important information such as block size and replication
```

In the core-site.xml file we can find some important information
```xml
  <configuration>

    <property>
      <name>fs.azure.user.agent.prefix</name>
      <value>User-Agent: APN/1.0 Hortonworks/1.0 HDP/2.6.5.0-292</value>
    </property>

    <property>
      <name>fs.defaultFS</name>
      <value>hdfs://nn01.itversity.com:8020</value> ---------> using this url in the browser we can connect to the cluster
      <final>true</final>
    </property>  <configuration>

    <property>
      <name>fs.azure.user.agent.prefix</name>
      <value>User-Agent: APN/1.0 Hortonworks/1.0 HDP/2.6.5.0-292</value>
    </property>

    <property>
      <name>fs.defaultFS</name>
      <value>hdfs://nn01.itversity.com:8020</value> ---------> using this url in the browser we can connect to the cluster
      <final>true</final>
    </property>
```

However, if we have launched the cluster managed by yarn, we can access that in the /etc/hadoop/conf/yarn-site.xml
```xml
<property>
      <name>yarn.resourcemanager.webapp.address</name>
      <value>rm01.itversity.com:19088</value>    ---------> we can copy in the browser to access to the cluster
    </property>
```

### Copy a file from local to the cluster

```console
hadoop fs -copyFromLocal originPath destinationPath
```
or using the put command (they are equivalent)

```console
hadoop fs -put originPath destinationPath
```


### Copy from cluster to the local file system

Example

```console
hadoop fs -copyToLocal /originPath destinationPath
```
or

```console
hadoop fs -get originPath destinationPath
```

### Copy one file from one HDFS location to another

```console
hadoop fs -cp
```

### Listing files in the cluster
```console
hadoop fs -ls
```

### Checking size of files
```console
hadoop fs -du
```


## Dataframe:  distributed collection with structure (RDD with structure)

### Reading data using SQLContext
SQLContext have 2 APIs to read data of different file formats

* load – typically takes 2 arguments, path and format
* read – have an interface for each of the file formats (e.g.: read.json)

Following are the file formats supported

- text
- orc
- parquet
- json (example showed)
- csv (3rd party plugin)
- avro (3rd party plugin, but Cloudera clusters get by default)

```python
sqlContext.load("/public/retail_db_json/order_items", "json").show()
sqlContext.read.json("/public/retail_db_json/order_items").show()
```

Before extracting information from a dataset or RDD, we have to find out the type of the file read

```python
orders = sc.textFile("/public/retail_db/orders")
orders.show()

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


## Working with RDDs

Quite often we might have to read data from the local file system and then create RDD out of it to process in conjunction with other RDDs. Here are the steps to create RDD using data from files in local file system.


1. Read data from files using Python File I/O APIs
2. Create collection out of it
3. Convert into RDD using sc.parallelize by passing the collection as an argument
4. Now you will have data in the form of RDD which can be processed using Spark APIs

```python
# we read from the local file systems and split it in lines
productsRaw = open("/data/retail_db/products/part-00000").read().splitlines()
type(productsRaw)
# we use parallelize to convert it to a RDD
productsRDD = sc.parallelize(productsRaw)
```



** Dataframe:  distributed collection with structure (RDD with structure) **

## Reading data using SQLContext

SQLContext have 2 APIs to read data of different file formats

1. load – typically takes 2 arguments, path and format
2. read – have an interface for each of the file formats (e.g.: read.json)

Following are the file formats supported

- text
- orc
- parquet
- json (example showed)
- csv (3rd party plugin)
- avro (3rd party plugin, but Cloudera clusters get by default)

```python
sqlContext.load("/public/retail_db_json/order_items", "json").show()
sqlContext.read.json("/public/retail_db_json/order_items").show()
```
Before extracting information from a dataset or RDD, we have to find out the type of the file read
```python

orders = sc.textFile("/public/retail_db/orders")
>>> type(orders.first())
<type 'unicode'>


mystring = s.split(",")
help(mystring) -----> we will be able to access to possible methods in the data structure)
```

## Basic operations

### Flatmap
```python
linesList = ["How are you", "let us perform", "word count using flatMap", "to understand flatMap in detail"]

# we convert it to a RDD
lines = sc.parallelize(linesList)


words = lines.flatMap(lambda l: l.split(" "))
tuples = words.map(lambda word: (word, 1))

# we print the tuple
for i in tuples.countByKey(): print(i)
```


### Filtering
```python
orders = sc.textFile("/public/retail_db/orders")
ordersComplete = orders. \
filter(lambda o: 
  o.split(",")[3] in ["COMPLETE", "CLOSED"] and o.split(",")[1][:7] == "2014-01") 
```


### Joins
When performing a join operation, when we are working with joins, it is better to perform the join by using integers

```python
orders = sc.textFile("/public/retail_db/orders")
orderItems = sc.textFile("/public/retail_db/order_items")

ordersMap = orders. \
map(lambda o:(int(o.split(",")[0]), o.split(",")[1]))

orderItemsMap = orderItems. \
map(lambda oi:(int(oi.split(",")[1]), float(oi.split(",")[4])))

ordersJoin = ordersMap.join(orderItemsMap)

for i in ordersJoin.take(10): print(i)
```


### Outer joins
```python
orders = sc.textFile("/public/retail_db/orders")
orderItems = sc.textFile("/public/retail_db/order_items")

ordersMap = orders. \
map(lambda o:(int(o.split(",")[0]), o.split(",")[3]))

orderItemsMap = orderItems. \
map(lambda oi:(int(oi.split(",")[1]), float(oi.split(",")[4])))

ordersLeftOuterJoin = ordersMap.leftOuterJoin(orderItemsMap)

ordersLeftOuterJoinFilter = ordersLeftOuterJoin. \
filter(lambda o: o[1][1] == None)

for i in ordersLeftOuterJoin.take(10): print(i)

ordersRightOuterJoin = orderItemsMap.rightOuterJoin(ordersMap)
ordersRightOuterJoinFilter = ordersRightOuterJoin. \
filter(lambda o: o[1][0] == None)

for i in ordersRightOuterJoinFilter.take(10): print(i)
```
- In most of the cases, joins receive maps and returns map structures.
- Left Outer join means data that is not present in another table----> parent table has to be on the left 
- Right outer join must be on the right
- Outerjoin are usually followed by map or filter

### Aggregations
There are several APIs to perform aggregations

1. Total aggregations – collect, reduce
2. ByKey aggregations – reduceByKey and aggregateByKey
3. groupByKey can be used for aggregations but should be given low priority as it does not use the combiner

Example:
```python
>>> orderItems = sc.textFile("/public/retail_db/order_items")
>>> for i in orderItems.take(10): print(i)
... 
 1,1,957,1,299.98,299.98
2,2,1073,1,199.99,199.99  <----
3,2,502,5,250.0,50.0 <----
4,2,403,1,129.99,129.99 <----
5,4,897,2,49.98,24.99
```

**Exercise:** we would like to get the total revenue for item with key 2
Steps:
1. we need to extract the items with key= 2
(so we need to filter):
```python
orderItemsFiltered = orderItems. \
    filter(lambda oi: int(oi.split(",")[1])==2)

>>> for i in orderItemsFiltered.take(10): print(i)
... 

2,2,1073,1,199.99,199.99
3,2,502,5,250.0,50.0
4,2,403,1,129.99,129.99
```

2. extract items subtotal
```python
orderItemsSubtotal = orderItemsFiltered.map(lambda oi: float(oi.split(",")[4]))
```

3. The subtotal must be passed to reduce to get the total revenue
(First option):
```python
from operator import add
orderItemsSubtotals.reduce(add)

>>> orderItemsSubtotal.reduce(add)
579.98    
```

(Second option) - With a lambda function:

```python
orderItemsSubtotal.reduce(lambda x,y: x+y)

>>> orderItemsSubtotal.reduce(lambda x,y: x+y)
[Stage 16:>                                                         (0 + 0) / 2]
579.98                                                      
```

**Example 2**
Get order item details which have minimum order_item_subtotal for given order_id
```python
orderItems = sc.textFile("/public/retail_db/order_items")

1,1,957,1,299.98,299.98
2,2,1073,1,199.99,199.99
3,2,502,5,250.0,50.0
4,2,403,1,129.99,129.99 ---> the one having the least revenue subtotal is this one
```
Steps:

1. we have to filter the orderId we are looking for
```python
orderItemsFiltered = orderItems. \
    filter(lambda oi: int(oi.split(",")[1])==2)

>>> for i in orderItemsFiltered.take(10): print(i)
... 
2,2,1073,1,199.99,199.99
3,2,502,5,250.0,50.0
4,2,403,1,129.99,129.99
```

2. Now, we have to get the order revenue subtotal comparing to get the minimum one. We don't need to use the map function as we do not want to get another data structure.
```python
orderItemsFiltered. \
reduce(lambda x, y: 
       x if(float(x.split(",")[4]) < float(y.split(",")[4])) else y
      )
u'4,2,403,1,129.99,129.99'                                                      
```


3. get count by status - countByKey
```python
orders = sc.textFile("/public/retail_db/orders")
>>> for i in orders.take(10): print(i)
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


# as we are trying to get the count by status, the status is the key
ordersStatus = orders. \
map(lambda o: (o.split(",")[3], 1))
countByStatus = ordersStatus.countByKey()

for i in countByStatus: print(i)
ordersStatus = orders. \
map(lambda o: (o.split(",")[3],1))

>>> ordersStatus = orders. \
... map(lambda o: (o.split(",")[3],1))
>>> for i in ordersStatus.take(10): print(i)
... 
(u'CLOSED', 1)
(u'PENDING_PAYMENT', 1)
(u'COMPLETE', 1)
(u'CLOSED', 1)
(u'COMPLETE', 1)
(u'COMPLETE', 1)
(u'COMPLETE', 1)
(u'PROCESSING', 1)
(u'PENDING_PAYMENT', 1)
(u'PENDING_PAYMENT', 1)
>>> 



#As  countbykey is an action, it will execute inmediately. The exit will be a python collection, not a spark RDD
>>> countByStatus = ordersStatus.countByKey()
>>> countByStatus                                                               
defaultdict(<type 'int'>, {u'COMPLETE': 22899, u'PAYMENT_REVIEW': 729, u'PROCESSING': 8275, u'CANCELED': 1428, u'PENDING': 7610, u'CLOSED': 7556, u'PENDING_PAYMENT': 15030, u'SUSPECTED_FRAUD': 1558, u'ON_HOLD': 3798})


```
reduceByKey, groupBykey, sortByKey are transformations and the result will be a RDD. reduceByKey or aggregateByKey use a combiner but groupByKey does not use it. 


### Aggregations - groupByKey

1,(1 to 1000) - sum(1 to 1000) => 1+2+4+...1000 (groupByKey approach)
1,(1 to 1000) - sum(sum(1,250) sum(251,500) sum(501,750) sum(751,1000)) ----> dividing the sum in subsets -> we will divide the execution in 4 tasks and then, we will compute the total. Is is faster as it uses more resources to compute in paralell (reduceByKey or aggregateByKey approach. This is thanks to the use of the combiner)

If the intermediate results are the same we can use reduceByKey (as we have an addition, the intermediate results are the same)---> It will be used to compute the intermediate values as well as the final value

If the intermediate results are different from those to compute the final result, we have to use aggregateByKey



**Problem** Get revenue for each order_id - groupByKey
Steps:
1. we read the data
```python
orderItems= sc.textFile.read('/public/retail_db/order_items')
>>> for i in orderItems.take(10): print(i)
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
```
(the order is the second column and the revenue is the 5 th column)

```python
orderItemsMap = orderItems. \
map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))

>>> for i in orderItemsMap.take(10): print(i)
... 
(1, 299.98)
(2, 199.99)
(2, 250.0)
(2, 129.99)
(4, 49.98)
(4, 299.95)
(4, 150.0)
(4, 199.92)
(5, 299.98)
(5, 299.95)

```
As we can see, we have the orderId as the first element and the revenue as the second element of the tuple)


```python
orderItemsGroupByOrderId = orderItemsMap.groupByKey()
```

For each revenue it will convert the element into a key and an array of values (2, [199.99,250.0, 129.99]) and then taking the list [199.99,250.0, 129.99] we can perform any list operations we would like to perform on it
```python
>>> for i in orderItemsGroupByOrderId.take(10): print(i)
... 
(2, <pyspark.resultiterable.ResultIterable object at 0x7f2a85fcd190>)           
(4, <pyspark.resultiterable.ResultIterable object at 0x7f2a85fcd350>)
(8, <pyspark.resultiterable.ResultIterable object at 0x7f2a85fcd390>)
(10, <pyspark.resultiterable.ResultIterable object at 0x7f2a85fcd3d0>)
(12, <pyspark.resultiterable.ResultIterable object at 0x7f2a85fcd410>)
(14, <pyspark.resultiterable.ResultIterable object at 0x7f2a85fcd450>)
(16, <pyspark.resultiterable.ResultIterable object at 0x7f2a85fcd490>)
(18, <pyspark.resultiterable.ResultIterable object at 0x7f2a85fcd4d0>)
(20, <pyspark.resultiterable.ResultIterable object at 0x7f2a85fcd510>)
(24, <pyspark.resultiterable.ResultIterable object at 0x7f2a85fcd550>)
orderItemsGroupByOrderId.first()
l =  orderItemsGroupByOrderId.first()
>>> type(l)
<type 'tuple'>
>>> l[0]
2
>>> list(l[1])
[199.99, 250.0, 129.99]

>>> sum(l[1])
579.98

# now, we perform the same logic with all the elements
revenuePerOrderId = orderItemsGroupByOrderId.map(lambda oi: (oi[0], sum(oi[1])))
>>> for i in revenuePerOrderId.take(10): print(i)
... 
(2, 579.98)
(4, 699.85)
(8, 729.8399999999999)
(10, 651.9200000000001)
(12, 1299.8700000000001)
(14, 549.94)
(16, 419.93)
(18, 449.96000000000004)
(20, 879.8599999999999)
(24, 829.97)
```
if we want the total revenue to be cleaner, we have to use the round function
```
revenuePerOrderId = orderItemsGroupByOrderId.map(lambda oi: (oi[0], round(sum(oi[1])))
```


**Problem**
Get order items detail in descending order by revenue - groupByKey

1. we read the data
```python
orderItems= sc.textFile.read('/public/retail_db/order_items')
>>> for i in orderItems.take(10): print(i)
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

>>> orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]),oi))
>>> for i in orderItemsMap.take(10):print(i)
... 
(1, u'1,1,957,1,299.98,299.98')
(2, u'2,2,1073,1,199.99,199.99')
(2, u'3,2,502,5,250.0,50.0')
(2, u'4,2,403,1,129.99,129.99')
(4, u'5,4,897,2,49.98,24.99')
(4, u'6,4,365,5,299.95,59.99')
(4, u'7,4,502,3,150.0,50.0')
(4, u'8,4,1014,4,199.92,49.98')
(5, u'9,5,957,1,299.98,299.98')
(5, u'10,5,365,5,299.95,59.99')


>>> for i in orderItemsGroupByOrderId.take(10):print(i)
... 
[Stage 3:>                                                          (0 + 2) / 2]
(2, <pyspark.resultiterable.ResultIterable object at 0x7ff8e6a7d590>)           
(4, <pyspark.resultiterable.ResultIterable object at 0x7ff8e6a7d6d0>)
(8, <pyspark.resultiterable.ResultIterable object at 0x7ff8e6a7d810>)
(10, <pyspark.resultiterable.ResultIterable object at 0x7ff8e6a7d790>)
(12, <pyspark.resultiterable.ResultIterable object at 0x7ff8e6a7d7d0>)
(14, <pyspark.resultiterable.ResultIterable object at 0x7ff8e6a7d910>)
(16, <pyspark.resultiterable.ResultIterable object at 0x7ff8e6a7d890>)
(18, <pyspark.resultiterable.ResultIterable object at 0x7ff8e6a7d850>)
(20, <pyspark.resultiterable.ResultIterable object at 0x7ff8e6a7d990>)
(24, <pyspark.resultiterable.ResultIterable object at 0x7ff8e6a7d950>)

>>> l = orderItemsGroupByOrderId.first()

>>> 
>>> list(l[1])
[u'2,2,1073,1,199.99,199.99', u'3,2,502,5,250.0,50.0', u'4,2,403,1,129.99,129.99']
```
As we can see above, there are three records associated with each key. If we take a look to the sorted function we can see:
Help on built-in function sorted in module __builtin__:

### Help

```console
sorted(...)
    sorted(iterable, cmp=None, key=None, reverse=False) --> new sorted list
```
As we can see, as the "key" parameter we can pass a lambda function, and we can apply a logic for what it will be sorted. So...

```console
>>> sorted(l[1],key = lambda k: float(k.split(",")[4]), reverse = True)
[u'3,2,502,5,250.0,50.0', u'2,2,1073,1,199.99,199.99', u'4,2,403,1,129.99,129.99']
```

Above, we can see that the revenue is ordered in descending order by revenue. Now we have to apply the same for each order, as we have just done it for the 
```python
first order (l[1])

>>> orderItemsSortedBySubtotalPerOrder =  orderItemsGroupByOrderId. \
... map(lambda oi: sorted(oi[1],key = lambda k: float(k.split(",")[4]), reverse = True))

>>> for i in orderItemsSortedBySubtotalPerOrder.take(10):print(i)
... 
[u'3,2,502,5,250.0,50.0', u'2,2,1073,1,199.99,199.99', u'4,2,403,1,129.99,129.99']
[u'6,4,365,5,299.95,59.99', u'8,4,1014,4,199.92,49.98', u'7,4,502,3,150.0,50.0', u'5,4,897,2,49.98,24.99']
[u'18,8,365,5,299.95,59.99', u'19,8,1014,4,199.92,49.98', u'17,8,365,3,179.97,59.99', u'20,8,502,1,50.0,50.0']
[u'24,10,1073,1,199.99,199.99', u'28,10,1073,1,199.99,199.99', u'26,10,403,1,129.99,129.99', u'25,10,1014,2,99.96,49.98', u'27,10,917,1,21.99,21.99']
[u'37,12,191,5,499.95,99.99', u'34,12,957,1,299.98,299.98', u'38,12,502,5,250.0,50.0', u'36,12,1014,3,149.94,49.98', u'35,12,134,4,100.0,25.0']
[u'40,14,1004,1,399.98,399.98', u'41,14,1014,2,99.96,49.98', u'42,14,502,1,50.0,50.0']
[u'49,16,365,5,299.95,59.99', u'48,16,365,2,119.98,59.99']
[u'55,18,1073,1,199.99,199.99', u'57,18,403,1,129.99,129.99', u'56,18,365,2,119.98,59.99']
[u'63,20,365,5,299.95,59.99', u'60,20,502,5,250.0,50.0', u'61,20,1014,4,199.92,49.98', u'62,20,403,1,129.99,129.99']
[u'71,24,502,5,250.0,50.0', u'72,24,1073,1,199.99,199.99', u'73,24,1073,1,199.99,199.99', u'69,24,403,1,129.99,129.99', u'70,24,502,1,50.0,50.0']
```
As we can see, the results are ordered by revenue in descending order

If we would like to print each order individually, we should use flatmap instead of map, because map returns an array of values and we would like to flatten that array to return an element per line

```python
>>> orderItemsSortedBySubtotalPerOrder =  orderItemsGroupByOrderId. \
... flatMap(lambda oi: sorted(oi[1],key = lambda k: float(k.split(",")[4]), reverse = True))


>>> for i in orderItemsSortedBySubtotalPerOrder.take(10):print(i)
... 
3,2,502,5,250.0,50.0
2,2,1073,1,199.99,199.99
4,2,403,1,129.99,129.99
6,4,365,5,299.95,59.99
8,4,1014,4,199.92,49.98
7,4,502,3,150.0,50.0
5,4,897,2,49.98,24.99
18,8,365,5,299.95,59.99
19,8,1014,4,199.92,49.98
17,8,365,3,179.97,59.99
```
### reduceByKey
It is used for perfoming aggregations

When we read the documentation we can see:

```python
reduceByKey(func, [numTasks])
```
When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values for each key are aggregated using the given reduce function func, which must be of type (V,V) => V. Like in groupByKey, the number of reduce tasks is configurable through an optional second argument.

With the lambda function you tell what will be done in each of every value of the given key
```python

orderItems = sc.textFile("/public/retail_db/order_items")
orderItemsMap = orderItems. \
map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))

>>> for i in orderItemsMap.take(10):print(i)
... 
(1, 299.98)
(2, 199.99)
(2, 250.0)
(2, 129.99)
(4, 49.98)
(4, 299.95)
(4, 150.0)
(4, 199.92)
(5, 299.98)
(5, 299.95)

```
Now, when we use reduceByKey, internally the records should be grouped for each key. Whatever lambda function you pass to the reduceByKey function, it will be executed in each value pair.
1.First method

```python
revenuePerOrderId = orderItemsMap.reduceByKey(lambda x,y: x+y)
```

2. Second method
```python
from operator import add

revenuePerOrderId = orderItemsMap.reduceByKey(add)
>>> for i in revenuePerOrderId.take(10):print(i)
... 
[Stage 12:>                                                         (0 + 0) / 2]

(2, 579.98)                                                                     
(4, 699.85)
(8, 729.8399999999999)
(10, 651.9200000000001)
(12, 1299.8700000000001)
(14, 549.94)
(16, 419.93)
(18, 449.96000000000004)
(20, 879.8599999999999)
(24, 829.97)
```

**Problem**
Get min revenue for each orderID
```python
minSubtotalPerOrderId = orderItemsMap.reduceByKey(lambda x, y: x if(x<y) else y)

>>> minSubtotalPerOrderId = orderItemsMap.reduceByKey(lambda x, y: x if(x<y) else y)

>>> 
>>> for i in minSubtotalPerOrderId.take(10): print(i)
... 
(2, 129.99)                                                                     
(4, 49.98)
(8, 50.0)
(10, 21.99)
(12, 100.0)
(14, 50.0)
(16, 119.98)
(18, 119.98)
(20, 129.99)
(24, 50.0)
```


**Problem**
Get order item details with minimum subtotal for each order_id


```python
orderItems = sc.textFile("/public/retail_db/order_items")
orderItemsMap = orderItems. \
map(lambda oi: (int(oi.split(",")[1]), oi))

# in this case x, y are string and Y will have all the values in the row as a string
# we get the revenue (4th element in the string)
>>> minSubtotalPerOrderId = orderItemsMap. \
... reduceByKey(lambda x, y: 
...   x if(float(x.split(",")[4]) < float(y.split(",")[4])) else y
...   )

>>> for i in minSubtotalPerOrderId.take(10): print(i)
... 
(2, u'4,2,403,1,129.99,129.99')                                                 
(4, u'5,4,897,2,49.98,24.99')
(8, u'20,8,502,1,50.0,50.0')
(10, u'27,10,917,1,21.99,21.99')
(12, u'35,12,134,4,100.0,25.0')
(14, u'42,14,502,1,50.0,50.0')
(16, u'48,16,365,2,119.98,59.99')
(18, u'56,18,365,2,119.98,59.99')
(20, u'62,20,403,1,129.99,129.99')
(24, u'70,24,502,1,50.0,50.0')

```


### aggregateByKey

We use it when intermediate and final values are different

**Problem**
Get revenue and count of items for each order_id

```python
>>> orderItems = sc.textFile("/public/retail_db/order_items")
```
If we show the data:

```python
>>> for i in orderItems.take(10): print(i)
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
```

1. Firstly, we get the order_id and revenue
```python
>>> orderItemsMap = orderItems. \
... map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))
>>> for i in orderItemsMap.take(10): print(i)
... 
(1, 299.98)
(2, 199.99)
(2, 250.0)
(2, 129.99)
(4, 49.98)
(4, 299.95)
(4, 150.0)
(4, 199.92)
(5, 299.98)
(5, 299.95)
```

2. We would like to have an exit like this:
(1, (299.98,1))
(2, (579.98,3))


As far as the touple is concerned, we are trying to get the revenue and count in one operation, we cannot use reduceByKey. reduceByKey demands the input value type and the output value type to be the same.

(0.0, 0) ---> the initialization
the second element of aggregateByKey is of type (tuple,int) ---> x is of type tuple and y is of type float
As x is of type tuple, if we want to add the first element in the tuple to the "y" value, we have to select the element in the tuple by the index ----> x[0] + y, x[1] +1

example:
(2, 199.99)
(2, 250.0)
(2, 129.99)
1st iteration of intermediate values (input value types): (0.0, 0), 199.99 ----> (199.99,1) 
((0.0, 0), 199.99 x will be (0.0, 0) and y will be 199.99
2nd iteration of intermediate values (input value types): (199.99,1), 250.0 -> (449.99,2)

Finally, what we want is this (output)
(2,(129.99,1))
(2,(449.99,2))
(2,(579.98,3))

```python
revenuePerOrder =  orderItemsMap. \
aggregateByKey((0.0, 0),
    lambda x,y: (x[0] + y, x[1] +1),
    lambda x,y: (x[0] + y[0], x[1] + y[1]))


>>> for i in revenuePerOrder.take(10):print(i)
... 
(2, (579.98, 3))                                                                
(4, (699.85, 4))
(8, (729.8399999999999, 4))
(10, (651.9200000000001, 5))
(12, (1299.8700000000001, 5))
(14, (549.94, 3))
(16, (419.93, 2))
(18, (449.96000000000004, 3))
(20, (879.8599999999999, 4))
(24, (829.97, 5))
```
### sortByKey
**Problem**
Sort data by product price - sortByKey
```python
products = sc.textFile("/public/retail_db/products")
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

# price is the index 4 of the string (5 th element)
productsMap = products. \
filter(lambda p: p.split(",")[4] != ""). \
map(lambda p: (float(p.split(",")[4]), p))
```
(the key has to be what we want the data to be sorted on, so in this case it will be the price)

```python
>>> for i in productsMap.take(10):print(i)
... 
(59.98, u'1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy')
(129.99, u"2,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat")
(89.99, u"3,2,Under Armour Men's Renegade D Mid Football Cl,,89.99,http://images.acmesports.sports/Under+Armour+Men%27s+Renegade+D+Mid+Football+Cleat")
(89.99, u"4,2,Under Armour Men's Renegade D Mid Football Cl,,89.99,http://images.acmesports.sports/Under+Armour+Men%27s+Renegade+D+Mid+Football+Cleat")
(199.99, u'5,2,Riddell Youth Revolution Speed Custom Footbal,,199.99,http://images.acmesports.sports/Riddell+Youth+Revolution+Speed+Custom+Football+Helmet')
(134.99, u"6,2,Jordan Men's VI Retro TD Football Cleat,,134.99,http://images.acmesports.sports/Jordan+Men%27s+VI+Retro+TD+Football+Cleat")
(99.99, u'7,2,Schutt Youth Recruit Hybrid Custom Football H,,99.99,http://images.acmesports.sports/Schutt+Youth+Recruit+Hybrid+Custom+Football+Helmet+2014')
(129.99, u"8,2,Nike Men's Vapor Carbon Elite TD Football Cle,,129.99,http://images.acmesports.sports/Nike+Men%27s+Vapor+Carbon+Elite+TD+Football+Cleat")
(50.0, u'9,2,Nike Adult Vapor Jet 3.0 Receiver Gloves,,50.0,http://images.acmesports.sports/Nike+Adult+Vapor+Jet+3.0+Receiver+Gloves')
(129.99, u"10,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat")
```

And we sort the data by key:
```python
>>> productsSortedByPrice = productsMap.sortByKey()
>>> for i in productsSortedByPrice.take(10):print(i)
... 
(0.0, u"38,3,Nike Men's Hypervenom Phantom Premium FG Socc,,0.0,http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat")
(0.0, u"388,18,Nike Men's Hypervenom Phantom Premium FG Socc,,0.0,http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat")
(0.0, u"414,19,Nike Men's Hypervenom Phantom Premium FG Socc,,0.0,http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat")
(0.0, u"517,24,Nike Men's Hypervenom Phantom Premium FG Socc,,0.0,http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat")
(0.0, u"547,25,Nike Men's Hypervenom Phantom Premium FG Socc,,0.0,http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat")
(0.0, u'934,42,Callaway X Hot Driver,,0.0,http://images.acmesports.sports/Callaway+X+Hot+Driver')
(0.0, u"1284,57,Nike Men's Hypervenom Phantom Premium FG Socc,,0.0,http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat")
(4.99, u'624,29,adidas Batting Helmet Hardware Kit,,4.99,http://images.acmesports.sports/adidas+Batting+Helmet+Hardware+Kit')
(4.99, u'815,37,Zero Friction Practice Golf Balls - 12 Pack,,4.99,http://images.acmesports.sports/Zero+Friction+Practice+Golf+Balls+-+12+Pack')
(5.0, u'336,15,"Nike Swoosh Headband - 2""",,5.0,http://images.acmesports.sports/Nike+Swoosh+Headband+-+2%22')
```

Now we extract the data we want from the previous steps(discarding the key, because we don0t need it

```python
>>> productsSortedMap = productsSortedByPrice. \
... map(lambda p: p[1])
>>> 
>>> for i in productsSortedMap.take(10): print(i)
... 
934,42,Callaway X Hot Driver,,0.0,http://images.acmesports.sports/Callaway+X+Hot+Driver
1284,57,Nike Men's Hypervenom Phantom Premium FG Socc,,0.0,http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat
38,3,Nike Men's Hypervenom Phantom Premium FG Socc,,0.0,http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat
388,18,Nike Men's Hypervenom Phantom Premium FG Socc,,0.0,http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat
414,19,Nike Men's Hypervenom Phantom Premium FG Socc,,0.0,http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat
517,24,Nike Men's Hypervenom Phantom Premium FG Socc,,0.0,http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat
547,25,Nike Men's Hypervenom Phantom Premium FG Socc,,0.0,http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat
815,37,Zero Friction Practice Golf Balls - 12 Pack,,4.99,http://images.acmesports.sports/Zero+Friction+Practice+Golf+Balls+-+12+Pack
624,29,adidas Batting Helmet Hardware Kit,,4.99,http://images.acmesports.sports/adidas+Batting+Helmet+Hardware+Kit
336,15,"Nike Swoosh Headband - 2""",,5.0,http://images.acmesports.sports/Nike+Swoosh+Headband+-+2%22
```

**Problem**
Sort data by product category and then product price descending - sortByKey

```python
products = sc.textFile("/public/retail_db/products")
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

Taking into account this tuple:
1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy
```
We have to sort the data by category (2) and then, by productPrice (59,98). The result is a tuple, which has inside another tuple
((2,59.98), record)

```python
productsMap = products. \
filter(lambda p: p.split(",")[4] != ""). \
map(lambda p: ((int(p.split(",")[1]), -float(p.split(",")[4])), p))
((2, -59.98), u'1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy')
((2, -129.99), u"2,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat")
((2, -89.99), u"3,2,Under Armour Men's Renegade D Mid Football Cl,,89.99,http://images.acmesports.sports/Under+Armour+Men%27s+Renegade+D+Mid+Football+Cleat")
((2, -89.99), u"4,2,Under Armour Men's Renegade D Mid Football Cl,,89.99,http://images.acmesports.sports/Under+Armour+Men%27s+Renegade+D+Mid+Football+Cleat")
((2, -199.99), u'5,2,Riddell Youth Revolution Speed Custom Footbal,,199.99,http://images.acmesports.sports/Riddell+Youth+Revolution+Speed+Custom+Football+Helmet')
((2, -134.99), u"6,2,Jordan Men's VI Retro TD Football Cleat,,134.99,http://images.acmesports.sports/Jordan+Men%27s+VI+Retro+TD+Football+Cleat")
((2, -99.99), u'7,2,Schutt Youth Recruit Hybrid Custom Football H,,99.99,http://images.acmesports.sports/Schutt+Youth+Recruit+Hybrid+Custom+Football+Helmet+2014')
((2, -129.99), u"8,2,Nike Men's Vapor Carbon Elite TD Football Cle,,129.99,http://images.acmesports.sports/Nike+Men%27s+Vapor+Carbon+Elite+TD+Football+Cleat")
((2, -50.0), u'9,2,Nike Adult Vapor Jet 3.0 Receiver Gloves,,50.0,http://images.acmesports.sports/Nike+Adult+Vapor+Jet+3.0+Receiver+Gloves')
((2, -129.99), u"10,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat")
```


As we don't want the key (2, -59.98), we have to get the elements in each tuple (the rest element)

```python
>>> for i in productsMap. \
... sortByKey(). \
... map(lambda p: p[1]). \
... take(100): print(i)
... 
16,2,Riddell Youth 360 Custom Football Helmet,,299.99,http://images.acmesports.sports/Riddell+Youth+360+Custom+Football+Helmet
11,2,Fitness Gear 300 lb Olympic Weight Set,,209.99,http://images.acmesports.sports/Fitness+Gear+300+lb+Olympic+Weight+Set
5,2,Riddell Youth Revolution Speed Custom Footbal,,199.99,http://images.acmesports.sports/Riddell+Youth+Revolution+Speed+Custom+Football+Helmet
14,2,Quik Shade Summit SX170 10 FT. x 10 FT. Canop,,199.99,http://images.acmesports.sports/Quik+Shade+Summit+SX170+10+FT.+x+10+FT.+Canopy
12,2,Under Armour Men's Highlight MC Alter Ego Fla,,139.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Alter+Ego+Flash+Football...
23,2,Under Armour Men's Highlight MC Alter Ego Hul,,139.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Alter+Ego+Hulk+Football...
6,2,Jordan Men's VI Retro TD Football Cleat,,134.99,http://images.acmesports.sports/Jordan+Men%27s+VI+Retro+TD+Football+Cleat
2,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
8,2,Nike Men's Vapor Carbon Elite TD Football Cle,,129.99,http://images.acmesports.sports/Nike+Men%27s+Vapor+Carbon+Elite+TD+Football+Cleat
10,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
17,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
```

### Ranking
We can use take or takeOrdered methods 

**Problem**
Get top N products by price - Global Ranking - sortByKey and take

```python
>>> products = sc.textFile("/public/retail_db/products")

>>> for i in products.take(10): print(i)
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

The strategy is the next one:
map (to transform the data in tuples whose key is wanted to be sorted) => sortByKey => map(to discard the key) => take

One way to do a ranking is 1)sortByKey and 2)take the first n elements:
```python
productsMap = products. \
filter(lambda p: p.split(",")[4] != ""). \
map(lambda p: (float(p.split(",")[4]), p))
productsSortedByPrice = productsMap.sortByKey(False)
# As we want the data to be ordered in descending order by pryce, we pass "False" as an argument

for i in productsSortedByPrice. \
map(lambda p: p[1]). \
take(5): print(i)

>>> for i in productsSortedByPrice. \
... map(lambda p: p[1]). \
... take(5): print(i)
... 
208,10,SOLE E35 Elliptical,,1999.99,http://images.acmesports.sports/SOLE+E35+Elliptical
66,4,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill
199,10,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill
496,22,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill
1048,47,"Spalding Beast 60"" Glass Portable Basketball ",,1099.99,http://images.acmesports.sports/Spalding+Beast+60%22+Glass+Portable+Basketball+Hoop
```


### takeOrdered
Get the data in descending order
```python
products = sc.textFile("/public/retail_db/products")
productsFiltered = products. \
filter(lambda p: p.split(",")[4] != "")

>>> for i in productsFiltered.take(10):print(i)
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

# In the lambda we determine the criteria for the data to be ordered from
topNProducts = productsFiltered.top(5, key=lambda k: float(k.split(",")[4]))
>>> for i in topNProducts:print(i)
... 
208,10,SOLE E35 Elliptical,,1999.99,http://images.acmesports.sports/SOLE+E35+Elliptical
66,4,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill
199,10,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill
496,22,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill
1048,47,"Spalding Beast 60"" Glass Portable Basketball ",,1099.99,http://images.acmesports.sports/Spalding+Beast+60%22+Glass+Portable+Basketball+Hoop
```

### With takeOrdered:
(it will show duplicates as well)

```python
# as we have to sort it in descending order, we have to use the minus sign (-float(k.split(",")[4]))
topNProducts = productsFiltered. \
takeOrdered(5, key=lambda k: -float(k.split(",")[4]))
>>> for i in topNProducts: print(i)                                             
... 
208,10,SOLE E35 Elliptical,,1999.99,http://images.acmesports.sports/SOLE+E35+Elliptical
66,4,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill
199,10,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill
496,22,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill
1048,47,"Spalding Beast 60"" Glass Portable Basketball ",,1099.99,http://images.acmesports.sports/Spalding+Beast+60%22+Glass+Portable+Basketball+Hoop

```
Ranking - by-key --------------------------------------------> 64 no me entero
----------------
**Problem**
Get top N products by price per category

As part of by key or per group ranking once the data is grouped by the key we have to apply lambda function as part of flatMap to sort or rank the data. So, to sort the data we should be familiar with python based api’s to sort the collections.
```python
t = productsGroupByCategoryId.first()
```
If we want to sort the data in ascending order by the string use below command
```python
sorted(t[1])
```
To sort the using product price use below command
```python
sorted(t[1], key=lambda k: float(k.split(",")[4]), reverse=True)
```
As part of the lambda function that is passed to flatMap, we will invoke the function to get top N records from the array of values for each key.
```python
topNproductsByCategory = productsGroupByCategoryId. \ flatMap(lambda p:) sorted(t[1], key=lambda k: float(k.split(",")[4]), reverse=True)[:3] ) for i in topNProductsByCategory.take(10): print(i)
```


### SETS
we can do the next operations with sets:

  + union
  + intersection
  + minus (we have to do it with the "substraction" operation)

To use these operations, both datasets need to have the same fields


####Set operations - Prepare data - subsets of products for 2013-12 and 2014-01
```python
orders = sc.textFile("/public/retail_db/orders")
orderItems = sc.textFile("/public/retail_db/order_items")

orders201312 = orders. \
filter(lambda o: o.split(",")[1][:7] == "2013-12"). \
map(lambda o: (int(o.split(",")[0]), o))

orders201401 = orders. \
filter(lambda o: o.split(",")[1][:7] == "2014-01"). \
map(lambda o: (int(o.split(",")[0]), o))

orderItemsMap = orderItems. \
map(lambda oi: (int(oi.split(",")[1]), oi))
>>> for i in orderItemsMap.take(10):print(i)
... 
(1, u'1,1,957,1,299.98,299.98')
(2, u'2,2,1073,1,199.99,199.99')
(2, u'3,2,502,5,250.0,50.0')
(2, u'4,2,403,1,129.99,129.99')
(4, u'5,4,897,2,49.98,24.99')
(4, u'6,4,365,5,299.95,59.99')
(4, u'7,4,502,3,150.0,50.0')
(4, u'8,4,1014,4,199.92,49.98')
(5, u'9,5,957,1,299.98,299.98')
(5, u'10,5,365,5,299.95,59.99')

orderItems201312 = orders201312. \
join(orderItemsMap). \
map(lambda oi: oi[1][1])
orderItems201401 = orders201401. \
join(orderItemsMap). \
map(lambda oi: oi[1][1])



#Set operations - Union - Get product ids sold in 2013-12 and 2014-01
products201312 = orderItems201312. \
map(lambda p: int(p.split(",")[2]))
products201401 = orderItems201401. \
map(lambda p: int(p.split(",")[2]))

allproducts = products201312. \
union(products201401). \
distinct()



#Set operations - Intersection - Get product ids sold in both 2013-12 and 2014-01

products201312 = orderItems201312. \
map(lambda p: int(p.split(",")[2]))
products201401 = orderItems201401. \
map(lambda p: int(p.split(",")[2]))

commonproducts = products201312.intersection(products201401)



#Set operations - minus - Get product ids sold in 2013-12 but not in 2014-01

products201312only = products201312. \
subtract(products201401). \
distinct()

products201401only = products201401. \
subtract(products201312). \
distinct()
```





## SAVE RESULTS TO FILES
----------------------
### Saving as text files with delimiters - revenue per order id
```python
orderItems = sc.textFile("/public/retail_db/order_items")
orderItemsMap = orderItems. \
map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))
```

To format files, we have to utilize a map (to execute a lambda for each and every row of the dataset) and then, include the format logic inside the lambda function.
Example:

```python
t = (8, 729.839999999)
```
Now, we define the format we want (we would like the fields to be separated by a tab, not by a comma character
```
t[0] + "\t" + t[1]
```
As t[0] is of type int, t[1] is of type float and "\t" is of type string, when we try to execute the previous line, we get an error. Then, we have to convert, all the parts of the tuple to string type:
```python
str(t[0]) + "\t" + str(t[1])
```

If we want to limit the number of decimals, we can do it as follows:
```python
str(t[0]) + "\t" + str(round(t[1], 3))
from operator import add
revenuePerOrderId = orderItemsMap. \
reduceByKey(add). \
map(lambda r: str(r[0]) + "\t" + str(r[1]))
for i in revenuePerOrderId.take(100):print(i)
... 
2       579.98                                                                  
4   699.85
8   729.84
10  651.92
12  1299.87
14  549.94
16  419.93
18  449.96
20  879.86
24  829.97
28  1159.9
30  100.0
34  299.98
36  799.96

#if we want to save to a text file, we will have to specify a path that does not exist
revenuePerOrderId.saveAsTextFile("/user/carlos_sanchez/revenue_per_order_id")
```
If we check whether it has been created, we can see:
```python

[carlos_sanchez@gw03 ~]$ hadoop fs -ls /user/carlos_sanchez/revenue_per_order_id
Found 3 items
-rw-r--r--   2 carlos_sanchez hdfs          0 2020-07-10 04:32 /user/carlos_sanchez/revenue_per_order_id/_SUCCESS
-rw-r--r--   2 carlos_sanchez hdfs     368614 2020-07-10 04:32 /user/carlos_sanchez/revenue_per_order_id/part-00000
-rw-r--r--   2 carlos_sanchez hdfs     368684 2020-07-10 04:32 /user/carlos_sanchez/revenue_per_order_id/part-00001


>>> for i in sc.textFile("/user/carlos_sanchez/revenue_per_order_id").take(100): print(i)
... 
2   579.98
4   699.85
8   729.84
10  651.92
12  1299.87
14  549.94
16  419.93
18  449.96
20  879.86
24  829.97
28  1159.9
30  100.0
34  299.98
36  799.96
38  359.96
```


## COMPRESSION
Compression is used to reduce storage requirements in hadoop clusters ( this way the performance of the cluster will be better). TO know the supported compression types, we have to check a properties file in our cluster.
```python
[carlos_sanchez@gw03 ~]$ cd /etc/hadoop/conf
[carlos_sanchez@gw03 conf]$ ls
capacity-scheduler.xml      hadoop-env.cmd              hdfs-site.xml         kms-site.xml                mapred-site.xml           ssl-client.xml.example  topology_mappings.data
commons-logging.properties  hadoop-env.sh               health_check          log4j.properties            mapred-site.xml.template  ssl-server.xml          topology_script.py
configuration.xsl           hadoop-metrics2.properties  kms-acls.xml          mapred-env.cmd              secure                    ssl-server.xml.example  yarn-env.cmd
container-executor.cfg      hadoop-metrics.properties   kms-env.sh            mapred-env.sh               slaves                    taskcontroller.cfg      yarn-env.sh
core-site.xml               hadoop-policy.xml           kms-log4j.properties  mapred-queues.xml.template  ssl-client.xml            task-log4j.properties   yarn-site.xml
[carlos_sanchez@gw03 conf]$ vi core-site.xml 
```

(and then, we have to look for "codecs"-> "/codecs")

    <property>
      <name>io.compression.codecs</name>
      <value>org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.SnappyCodec</value>
    </property>

As we can see, the cluster admits Gzip and Snappy compression types, so we choose for example, the "org.apache.hadoop.io.compress.SnappyCodec" type
```python
revenuePerOrderId.saveAsTextFile("/user/carlos_sanchez/revenue_per_order_compressed", compressionCodecClass="org.apache.hadoop.io.compress.SnappyCodec")

[carlos_sanchez@gw03 conf]$ hadoop fs -ls /user/carlos_sanchez/revenue_per_order_compressed
Found 3 items
-rw-r--r--   2 carlos_sanchez hdfs          0 2020-07-10 05:08 /user/carlos_sanchez/revenue_per_order_compressed/_SUCCESS
-rw-r--r--   2 carlos_sanchez hdfs     206458 2020-07-10 05:08 /user/carlos_sanchez/revenue_per_order_compressed/part-00000.snappy
-rw-r--r--   2 carlos_sanchez hdfs     207515 2020-07-10 05:08 /user/carlos_sanchez/revenue_per_order_compressed/part-00001.snappy
```

To check whether the files have been saved succesfully in compressed format:
```python

>>> sc.textFile("/user/carlos_sanchez/revenue_per_order_compressed").take(10)

[u'2\t579.98', u'4\t699.85', u'8\t729.84', u'10\t651.92', u'12\t1299.87', u'14\t549.94', u'16\t419.93', u'18\t449.96', u'20\t879.86', u'24\t829.97']
```

If we want to print the contents of the files, we have to use a for loop.


## SAVING THE FILE IN OTHER FORMATS
Supported file formats
* orc
* json
* parquet
* avro (with databricks plugin)

Steps to save into different file formats
* Make sure data is represented as Data Frame
* Use write or save API to save Data Frame into different file formats
* Use compression algorithm if required


**Example**
Saving as JSON - Get revenue per order id
```python
orderItems = sc.textFile("/public/retail_db/order_items")
orderItemsMap = orderItems. \
map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))

from operator import add
revenuePerOrderId = orderItemsMap. \
reduceByKey(add). \
map(lambda r: (r[0], round(r[1], 2)))


#we show the content of the file (the data will be represented in a RDD of tuples):
>>> for i in revenuePerOrderId.take(100):print(i)
... 
2   579.98
4   699.85
8   729.84
10  651.92
12  1299.87
14  549.94
16  419.93
18  449.96
20  879.86
24  829.97
28  1159.9
30  100.0
34  299.98
36  799.96
38  359.96

# if we want to save in other formats, we have to convert the data into dataframes

revenuePerOrderIdDF = revenuePerOrderId.toDF(schema=["order_id", "order_revenue"])

>>> revenuePerOrderIdDF.show()
+--------+-------------+
|order_id|order_revenue|
+--------+-------------+
|       2|       579.98|
|       4|       699.85|
|       8|       729.84|
|      10|       651.92|
|      12|      1299.87|
|      14|       549.94|
|      16|       419.93|
|      18|       449.96|
|      20|       879.86|
|      24|       829.97|
|      28|       1159.9|
|      30|        100.0|
|      34|       299.98|
|      36|       799.96|
|      38|       359.96|
|      42|       739.92|
|      44|       399.98|
|      46|       229.95|
|      48|        99.96|
|      50|       429.97|
+--------+-------------+
```


Now we can save it to json format or whatever format we want
We have two options:
```python

revenuePerOrderIdDF.save("/user/dgadiraju/revenue_per_order_json", "json")
revenuePerOrderIdDF.write.json("/user/dgadiraju/revenue_per_order_json")
```


**IMPORTANT PROBLEM STATEMENT**

    - Use retail_db data set
    - Problem Statement
        * Get daily revenue by product considering completed and closed orders.
        * Data need to be sorted in ascending order by date and then descending order by revenue computed for each product for each day.
    - Data for orders and order_items is available in HDFS /public/retail_db/orders and /public/retail_db/order_items
    - Data for products is available locally under /data/retail_db/products
    - Final output need to be stored under
        * HDFS location – avro format /user/YOUR_USER_ID/daily_revenue_avro_python
        * HDFS location – text format /user/YOUR_USER_ID/daily_revenue_txt_python
        * Local location /home/YOUR_USER_ID/daily_revenue_python
        * Solution need to be stored under /home/YOUR_USER_ID/daily_revenue_python.txt


Solution:

1. We have to launch the spark shell and for that we have to undestand the environment and use resources optimally. For that we have to see the resource manager from the cluster interface. It will, tipically, be in port 8088. For the certification, we will be given the address of the cluster or they will bookmark it. 
If they do not give us such information, we can get it from:
    ```python
    [carlos_sanchez@gw03 conf]$ cd /etc/hadoop/conf
    [carlos_sanchez@gw03 conf]$ vi yarn-site.xml 
    ```

In the file, if we look for "/resourcemanager.webapp":

    <property>
      <name>yarn.resourcemanager.webapp.address</name>
      <value>rm01.itversity.com:19088</value> ----> and we can use it to the resource manager interface
    </property>

```python

    Then, in the cluster, we will se something like this:
        Apps Submitted  Apps Pending    ...     Memory Total     ...  VCores Total ...   Rebooted Nodes
            23687           0                       120GB                   60              0
```

We will need to focus on the column "Memory total" and "VCores Total" . Both columns will give us the capacity of the cluster. This way we will be able to calculate the number of executors that can be forked when launching the spark shell. We will not use the complete capacity of our cluster if the data is not big enough.
Then, we have to find the file size to determine the capacity of the cluster to optimally perform. For that we will use the command "du":
```python
    [carlos_sanchez@gw03 conf]$ du -s -h /data/retail_db/products
    176K    /data/retail_db/products
```
And we can see it is just 176 KB
In hadoop we have the command:

```python
    [carlos_sanchez@gw03 conf]$ hadoop fs -du -s -h /public/retail_db/orders
    2.9 M  /public/retail_db/orders
```

    And for order_items

    ```python
    [carlos_sanchez@gw03 conf]$ hadoop fs -du -s -h /public/retail_db/order_items
    5.2 M  /public/retail_db/order_items
    ```

As the data size is too small, we will not use the whole capacity. We will use 1 or 2 nodes to treat the data .At least we will have to need two task for each node (we can know the number of cluster by going to the "Nodes" tab in the cluster interface). There, all the nodes are listed (in the below side of the table, we can see the count of nodes). So, imagining we have 7 nodes -> 5 nodes * 2 tasks per node = 10 executors we have at total

  ```python
    pyspark --master yarn \
    --conf spark.ui.port=12890 \
    --num-executors 2 \
    --executor-memory 512M \
    --packages com.databricks:spark-avro_2.10:2.0.1
  ```


If we don't know the configuration, we can check the spark-submit file. Spark-submit is, primarily, used to submit the jobs

    ```python
    [carlos_sanchez@gw03 conf]$ spark-submit 
    Multiple versions of Spark are installed but SPARK_MAJOR_VERSION is not set
    Spark1 will be picked by default
    Usage: spark-submit [options] <app jar | python file> [app arguments]
    Usage: spark-submit --kill [submission ID] --master [spark://...]
    Usage: spark-submit --status [submission ID] --master [spark://...]
    ```

Options:
    ```python

      --master MASTER_URL         spark://host:port, mesos://host:port, yarn, or local.
      --deploy-mode DEPLOY_MODE   Whether to launch the driver program locally ("client") or
                                  on one of the worker machines inside the cluster ("cluster")
                                  (Default: client).
      --class CLASS_NAME          Your application's main class (for Java / Scala apps).
      --name NAME                 A name of your application.
      --jars JARS                 Comma-separated list of local jars to include on the driver
                                  and executor classpaths.
      --packages                  Comma-separated list of maven coordinates of jars to include
                                  on the driver and executor classpaths. Will search the local
                                  maven repo, then maven central and any additional remote
                                  repositories given by --repositories. The format for the
                                  coordinates should be groupId:artifactId:version.
      --exclude-packages          Comma-separated list of groupId:artifactId, to exclude while
                                  resolving the dependencies provided in --packages to avoid
                                  dependency conflicts.
    ```


2. We read orders and order_items
```python

    >>> orders = sc.textFile("/public/retail_db/orders")

    # we preview the data
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

    #also we will have to perform the count command to get how much data is going to be processed
    >>> orders.count()
    68883  


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
    >>> orderItems.count()
    172198
    >>> 
```

3. Filter COMPLETED and CLOSED orders:
The problem says we have to filter the complet and closed orders. For that, we have to look where is placed the state column in the data,
We don't know all the status items (ewhether all states are normalized). For that reason, we have to guess it:
```python
    # we take the third column, corresponding to the status
    # we will use collect to covert the RDD into a set
    >>> for i in orders.map(lambda o: o.split(",")[3]).distinct().collect():print(i)
    ... 
    PENDING
    SUSPECTED_FRAUD
    CLOSED
    ON_HOLD
    CANCELED
    PROCESSING
    PENDING_PAYMENT
    COMPLETE
    PAYMENT_REVIEW

    ordersFiltered = orders.filter(lambda o: o.split(",")[3] in ["COMPLETE","CLOSED"])
    >>> for i in ordersFiltered.take(10):print(i)
    ... 
    1,2013-07-25 00:00:00.0,11599,CLOSED
    3,2013-07-25 00:00:00.0,12111,COMPLETE
    4,2013-07-25 00:00:00.0,8827,CLOSED
    5,2013-07-25 00:00:00.0,11318,COMPLETE
    6,2013-07-25 00:00:00.0,7130,COMPLETE
    7,2013-07-25 00:00:00.0,4530,COMPLETE
    12,2013-07-25 00:00:00.0,1837,CLOSED
    15,2013-07-25 00:00:00.0,2568,COMPLETE
    17,2013-07-25 00:00:00.0,2667,COMPLETE
    18,2013-07-25 00:00:00.0,1205,CLOSED

    >>> ordersFiltered.count()
    30455
```

4. Convert both filtered orders and order_items to key value pairs
Now, from orders we have to only take date
```python

    # we preview the data
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


    As for orderItems, we need to take the 5th column, corresponding to the revenue. But on top, we need to do it on productId, which is the 2nd column

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
```

To sum up:
  - From orderItems we need productPrice, as well as productId
    Taking into account this line:
      1,1,957,1,299.98,299.98 

      -> 299.98: subtotal (revenue)
      -> productId: 957

      But to join both tables, we need to have a common key, which is orderId (second column in the table orderItems) and orderId in the table orders
      -> orderId: 1

  - From orders we need date and orderId

  And as far as order table is concerned, orderId is the first column
  1,2013-07-25 00:00:00.0,11599,CLOSED

      -> orderId: 1 
      -> date: 2013-07-25 00:00:00.0

```python

    ordersMap = ordersFiltered.map(lambda o: (int(o.split(",")[0]),o.split(",")[1]))

    >>> for i in ordersMap.take(10):print(i)
    ... 
    (1, u'2013-07-25 00:00:00.0')
    (3, u'2013-07-25 00:00:00.0')
    (4, u'2013-07-25 00:00:00.0')
    (5, u'2013-07-25 00:00:00.0')
    (6, u'2013-07-25 00:00:00.0')
    (7, u'2013-07-25 00:00:00.0')
    (12, u'2013-07-25 00:00:00.0')
    (15, u'2013-07-25 00:00:00.0')
    (17, u'2013-07-25 00:00:00.0')
    (18, u'2013-07-25 00:00:00.0')


    In orderItems:
    oi.split(",")[1] --> orderId ---> to join with order table
    oi.split(",")[2] --> productId --> because we are said we need to compute daily revenue per day per product
    oi.split(",")[4]

    we need two fields (orderItems Subtotal, product Id)
    orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]),(int(oi.split(",")[2]),float(oi.split(",")[4]))))
    >>> for i in orderItemsMap.take(10):print(i)
    ... 
    (1, (957, 299.98))
    (2, (1073, 199.99))
    (2, (502, 250.0))
    (2, (403, 129.99))
    (4, (897, 49.98))
    (4, (365, 299.95))
    (4, (502, 150.0))
    (4, (1014, 199.92))
    (5, (957, 299.98))
    (5, (365, 299.95))
```



5. Join the two datasets
```python

    ordersJoin = ordersMap.join(orderItemsMap)
    >>> for i in ordersJoin.take(10):print(i)
    ... 
    (65536, (u'2014-05-16 00:00:00.0', (957, 299.98)))                              
    (65536, (u'2014-05-16 00:00:00.0', (1014, 149.94)))
    (65536, (u'2014-05-16 00:00:00.0', (957, 299.98)))
    (65536, (u'2014-05-16 00:00:00.0', (1014, 149.94)))
    (4, (u'2013-07-25 00:00:00.0', (897, 49.98)))
    (4, (u'2013-07-25 00:00:00.0', (365, 299.95)))
    (4, (u'2013-07-25 00:00:00.0', (502, 150.0)))
    (4, (u'2013-07-25 00:00:00.0', (1014, 199.92)))
    (60076, (u'2013-10-22 00:00:00.0', (1004, 399.98)))
    (12, (u'2013-07-25 00:00:00.0', (957, 299.98)))
```

Using this information too have to get daily revenue per product. If I have to use an aggregate function as aggregateByyKey or reduceByKey, the key has to be date and productId ((daily revenue per product))

    
    from each tuple, we don't need the orderId -> 65536, so we can have tuples like:
        - '2014-05-16 00:00:00.0' & 957 as the key
        - 299.98 as the value
    (65536, (u'2014-05-16 00:00:00.0', (957, 299.98))) has to be transformed -> ((u'2014-05-16 00:00:00.0', 957), 299.98)

To transform it we can use the map function to pass the correct elements to the reduceByKey function to get each revenue per product

    ```python
    >>> t =(65536, (u'2014-05-16 00:00:00.0', (957, 299.98)))
    >>> t[0]
    65536
    >>> t[1]
    (u'2014-05-16 00:00:00.0', (957, 299.98))
    >>> t[1][1]
    (957, 299.98)
    >>> t[1][0]
    u'2014-05-16 00:00:00.0'


    # we have to define a tuple where the key is a tuple and the value is the daily revenue
    >>> ordersJoinMap = ordersJoin. \
    ...     map(lambda o: ((o[1][0], o[1][1][0]),o[1][1][1]))
    >>> for i in ordersJoinMap.take(10):print(i)
    ... 
    ((u'2014-05-16 00:00:00.0', 957), 299.98)
    ((u'2014-05-16 00:00:00.0', 1014), 149.94)
    ((u'2014-05-16 00:00:00.0', 957), 299.98)
    ((u'2014-05-16 00:00:00.0', 1014), 149.94)
    ((u'2013-07-25 00:00:00.0', 897), 49.98)
    ((u'2013-07-25 00:00:00.0', 365), 299.95)
    ((u'2013-07-25 00:00:00.0', 502), 150.0)
    ((u'2013-07-25 00:00:00.0', 1014), 199.92)
    ((u'2013-10-22 00:00:00.0', 1004), 399.98)
    ((u'2013-07-25 00:00:00.0', 957), 299.98)
    ```

Nnow, I can use reduce by key (the key) and then, aggregate the values,so if I want the subtotal I need t[1][1]


6. Get daily revenue per productId
```python 
    from operator import add
    dailyRevenuePerProductId = ordersJoinMap.reduceByKey(add)
    >>> for i in dailyRevenuePerProductId.take(10):print(i)
    ... 
    ((u'2014-03-26 00:00:00.0', 1073), 3799.8100000000004)                          
    ((u'2014-07-14 00:00:00.0', 1014), 2998.8)
    ((u'2014-06-21 00:00:00.0', 1073), 1199.94)
    ((u'2013-09-07 00:00:00.0', 703), 119.94)
    ((u'2014-01-25 00:00:00.0', 216), 189.0)
    ((u'2013-09-13 00:00:00.0', 1004), 5599.72)
    ((u'2014-01-02 00:00:00.0', 957), 2999.8)
    ((u'2014-04-15 00:00:00.0', 502), 3450.0)
    ((u'2014-05-15 00:00:00.0', 1073), 2399.88)
    ((u'2014-01-06 00:00:00.0', 249), 274.85)

```

As we can see data is aggregated by each day by productId. Now, we can use the map function the way we want.
Then, having productId we have to get product name (for doing that we have to read the data from the local file system)

7. Load products from local file system and convert int into RDD. The location in the local file system is /data/retail_db/products
```python

    [carlos_sanchez@gw03 ~]$ ls -ltr /data/retail_db/products
    total 172
    -rw-r--r-- 1 root root 174155 Feb 20  2017 part-00000


    productsRaw = open("/data/retail_db/products/part-00000").read().splitlines()
    >>> type(productsRaw)
    <type 'list'>
    >>> 
```

Once we know we have a list, we can convert it into a RDD. For that, paralellize will convert a collection into a RDD 
```python

       products = sc.parallelize(productsRaw)
    >>> products.take(10)
    ['1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy', "2,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat", "3,2,Under Armour Men's Renegade D Mid Football Cl,,89.99,http://images.acmesports.sports/Under+Armour+Men%27s+Renegade+D+Mid+Football+Cleat", "4,2,Under Armour Men's Renegade D Mid Football Cl,,89.99,http://images.acmesports.sports/Under+Armour+Men%27s+Renegade+D+Mid+Football+Cleat", '5,2,Riddell Youth Revolution Speed Custom Footbal,,199.99,http://images.acmesports.sports/Riddell+Youth+Revolution+Speed+Custom+Football+Helmet', "6,2,Jordan Men's VI Retro TD Football Cleat,,134.99,http://images.acmesports.sports/Jordan+Men%27s+VI+Retro+TD+Football+Cleat", '7,2,Schutt Youth Recruit Hybrid Custom Football H,,99.99,http://images.acmesports.sports/Schutt+Youth+Recruit+Hybrid+Custom+Football+Helmet+2014', "8,2,Nike Men's Vapor Carbon Elite TD Football Cle,,129.99,http://images.acmesports.sports/Nike+Men%27s+Vapor+Carbon+Elite+TD+Football+Cleat", '9,2,Nike Adult Vapor Jet 3.0 Receiver Gloves,,50.0,http://images.acmesports.sports/Nike+Adult+Vapor+Jet+3.0+Receiver+Gloves', "10,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat"]
```

Now that we can see we have an RDD. Now we extract the information we want. In this case, we have to define productId as key and productName as value. 

If we print "products" data we can see:
```python

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

Taking into account the first row:
    1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy

    * productId is the first field: 1---> int(p.split(",")[0])
    * productName is the third fields: Quest Q64 10 FT. x 10 FT. Slant Leg Instant U ---> p.split(",")[2])

Now, we will create a tuple to join this data with the data we had previously obtained in step number 6
```python

    productsMap = products. \
    map(lambda p:(int(p.split(",")[0]),p.split(",")[2]))
```

If we print the tuple, we see:
```python

    >>> for i in productsMap.take(10):print(i)
    ... 
    (1, 'Quest Q64 10 FT. x 10 FT. Slant Leg Instant U')
    (2, "Under Armour Men's Highlight MC Football Clea")
    (3, "Under Armour Men's Renegade D Mid Football Cl")
    (4, "Under Armour Men's Renegade D Mid Football Cl")
    (5, 'Riddell Youth Revolution Speed Custom Footbal')
    (6, "Jordan Men's VI Retro TD Football Cleat")
    (7, 'Schutt Youth Recruit Hybrid Custom Football H')
    (8, "Nike Men's Vapor Carbon Elite TD Football Cle")
    (9, 'Nike Adult Vapor Jet 3.0 Receiver Gloves')
    (10, "Under Armour Men's Highlight MC Football Clea")
    >>> 
```

Now we can join this data with the data we had in step number 6 (daily revenue). We show the data below:
```python
    >>> for i in dailyRevenuePerProductId.take(10):print(i)
    ... 
    ((u'2014-03-26 00:00:00.0', 1073), 3799.8100000000004)                          
    ((u'2014-07-14 00:00:00.0', 1014), 2998.8)
    ((u'2014-06-21 00:00:00.0', 1073), 1199.94)
    ((u'2013-09-07 00:00:00.0', 703), 119.94)
    ((u'2014-01-25 00:00:00.0', 216), 189.0)
    ((u'2013-09-13 00:00:00.0', 1004), 5599.72)
    ((u'2014-01-02 00:00:00.0', 957), 2999.8)
    ((u'2014-04-15 00:00:00.0', 502), 3450.0)
    ((u'2014-05-15 00:00:00.0', 1073), 2399.88)
    ((u'2014-01-06 00:00:00.0', 249), 274.85)
```

As for this data, we have to chnage to get the columns we need:
```python
(u'2014-03-26 00:00:00.0', 1073), 3799.8100000000004) --> from this row, we have to make this format (1073, (u'2014-03-26 00:00:00.0', 3799.8100000000004)).

```

So now, what we have to do is to create that pattern.
```python

    t = (u'2014-03-26 00:00:00.0', 1073), 3799.8100000000004)
    t_map = (t[0][1], (t[0][0], t[1]))
    t_map
    (1073, (u'2014-03-26 00:00:00.0', 3799.8100000000004))

    >>> dailyRevenuePerProductIdMap = dailyRevenuePerProductId. \
    ... map(lambda r: (r[0][1], (r[0][0], r[1])))

    >>> for i in dailyRevenuePerProductIdMap.take(10): print(i)
    ... 
    (1073, (u'2014-03-26 00:00:00.0', 3799.8100000000004))                          
    (1014, (u'2014-07-14 00:00:00.0', 2998.8))
    (1073, (u'2014-06-21 00:00:00.0', 1199.94))
    (703, (u'2013-09-07 00:00:00.0', 119.94))
    (216, (u'2014-01-25 00:00:00.0', 189.0))
    (1004, (u'2013-09-13 00:00:00.0', 5599.72))
    (957, (u'2014-01-02 00:00:00.0', 2999.8))
    (502, (u'2014-04-15 00:00:00.0', 3450.0))
    (1073, (u'2014-05-15 00:00:00.0', 2399.88))
    (249, (u'2014-01-06 00:00:00.0', 274.85))

```
8. Join daily revenue per productId with products to get daily revenue per product in descending order.
```python

    dailyRevenuePerProductJoin = dailyRevenuePerProductIdMap.join(productsMap)

    >>> for i in dailyRevenuePerProductJoin.take(10): print(i)
    ... 
    (24, ((u'2013-12-04 00:00:00.0', 159.98), 'Elevation Training Mask 2.0'))
    (24, ((u'2013-08-18 00:00:00.0', 399.95), 'Elevation Training Mask 2.0'))
    (24, ((u'2013-10-06 00:00:00.0', 79.99), 'Elevation Training Mask 2.0'))
    (24, ((u'2014-06-11 00:00:00.0', 319.96), 'Elevation Training Mask 2.0'))
    (24, ((u'2013-11-02 00:00:00.0', 79.99), 'Elevation Training Mask 2.0'))
    (24, ((u'2013-11-19 00:00:00.0', 239.97), 'Elevation Training Mask 2.0'))
    (24, ((u'2013-08-26 00:00:00.0', 239.97), 'Elevation Training Mask 2.0'))
    (24, ((u'2013-10-12 00:00:00.0', 239.97), 'Elevation Training Mask 2.0'))
    (24, ((u'2013-12-18 00:00:00.0', 399.95), 'Elevation Training Mask 2.0'))
    (24, ((u'2014-03-20 00:00:00.0', 159.98), 'Elevation Training Mask 2.0'))


    (u'2013-12-04 00:00:00.0', 159.98), 'Elevation Training Mask 2.0'):

    u'2013-12-04 00:00:00.0' --> date
    159.98 -> revenue
    'Elevation Training Mask 2.0' -> productName
    24 -> we don't need it. We used it to join data
```

  In addition, we have to sort the data:
          + In ascending order by date
          + In descending order by revenue 
  And then, the output has to be in the format of - order_date, daily_revenue_per_product, product_name

9. Get data to desired format - order_date, daily_revenue_per_product, product_name.We define a variable to simplify:
```python

    t = (24, ((u'2013-12-04 00:00:00.0', 159.98), 'Elevation Training Mask 2.0'))
    >>> t[1]
    ((u'2013-12-04 00:00:00.0', 159.98), 'Elevation Training Mask 2.0')
```

For doing the sort per two criteria, the key have to be composite:
```python

    (u'2013-12-04 00:00:00.0', -159.98) -> we have to add "-" perform the sort in descending order
    
    >>> (t[1][0], t[1][1])
    ((u'2013-12-04 00:00:00.0', 159.98), 'Elevation Training Mask 2.0')

    >>> (t[1][0][0], -t[1][0][1])
    (u'2013-12-04 00:00:00.0', -159.98) ---> data will be sorted is ascending order by date and descending order by revenue- The problem is that we have to see the                                          revenue the way it is. In other words, revenue must be positive. Hence, we can do something like:

    >>> t[1][0][0] + "," + str(t[1][0][1]) + "," + t[1][1]
    u'2013-12-04 00:00:00.0,159.98,Elevation Training Mask 2.0'
```

And now we have the data the way we want. Now, as key we will choose (u'2013-12-04 00:00:00.0,159.98)
```python

    >>> t_map=((t[1][0][0], -t[1][0][1]),t[1][0][0] + "," + str(t[1][0][1]) + "," + t[1][1] )
    >>> t_map
    ((u'2013-12-04 00:00:00.0', -159.98), u'2013-12-04 00:00:00.0,159.98,Elevation Training Mask 2.0')
```
As we can see we have the negated key (for order issues) and then, as value we have u'2013-12-04 00:00:00.0,159.98,Elevation Training Mask 2.0', which is the concatenated string
```python
    dailyRevenuePerProduct = dailyRevenuePerProductJoin. \
    map(lambda t: ((t[1][0][0], -t[1][0][1]),t[1][0][0] + "," + str(t[1][0][1]) + "," + t[1][1] ))

    >>> for i in dailyRevenuePerProduct.take(10):print(i)
    ... 
    ((u'2013-12-04 00:00:00.0', -159.98), u'2013-12-04 00:00:00.0,159.98,Elevation Training Mask 2.0')
    ((u'2013-08-18 00:00:00.0', -399.95), u'2013-08-18 00:00:00.0,399.95,Elevation Training Mask 2.0')
    ((u'2013-10-06 00:00:00.0', -79.99), u'2013-10-06 00:00:00.0,79.99,Elevation Training Mask 2.0')
    ((u'2014-06-11 00:00:00.0', -319.96), u'2014-06-11 00:00:00.0,319.96,Elevation Training Mask 2.0')
    ((u'2014-04-15 00:00:00.0', -159.98), u'2014-04-15 00:00:00.0,159.98,Elevation Training Mask 2.0')
    ((u'2014-01-27 00:00:00.0', -159.98), u'2014-01-27 00:00:00.0,159.98,Elevation Training Mask 2.0')
    ((u'2014-04-02 00:00:00.0', -159.98), u'2014-04-02 00:00:00.0,159.98,Elevation Training Mask 2.0')
    ((u'2013-10-31 00:00:00.0', -399.95), u'2013-10-31 00:00:00.0,399.95,Elevation Training Mask 2.0')
    ((u'2014-04-06 00:00:00.0', -239.97), u'2014-04-06 00:00:00.0,239.97,Elevation Training Mask 2.0')
    ((u'2013-12-06 00:00:00.0', -239.97), u'2013-12-06 00:00:00.0,239.97,Elevation Training Mask 2.0')



    dailyRevenuePerProductSorted = dailyRevenuePerProduct.sortByKey()
    >>> for i  in dailyRevenuePerProductSorted.take(10): print(i)
    ... 
    ((u'2013-07-25 00:00:00.0', -5599.72), u'2013-07-25 00:00:00.0,5599.72,Field & Stream Sportsman 16 Gun Fire Safe')
    ((u'2013-07-25 00:00:00.0', -5099.489999999999), u"2013-07-25 00:00:00.0,5099.49,Nike Men's Free 5.0+ Running Shoe")
    ((u'2013-07-25 00:00:00.0', -4499.700000000001), u"2013-07-25 00:00:00.0,4499.7,Diamondback Women's Serene Classic Comfort Bi")
    ((u'2013-07-25 00:00:00.0', -3359.44), u'2013-07-25 00:00:00.0,3359.44,Perfect Fitness Perfect Rip Deck')
    ((u'2013-07-25 00:00:00.0', -2999.8500000000004), u'2013-07-25 00:00:00.0,2999.85,Pelican Sunstream 100 Kayak')
    ((u'2013-07-25 00:00:00.0', -2798.88), u"2013-07-25 00:00:00.0,2798.88,O'Brien Men's Neoprene Life Vest")
    ((u'2013-07-25 00:00:00.0', -1949.8500000000001), u"2013-07-25 00:00:00.0,1949.85,Nike Men's CJ Elite 2 TD Football Cleat")
    ((u'2013-07-25 00:00:00.0', -1650.0), u"2013-07-25 00:00:00.0,1650.0,Nike Men's Dri-FIT Victory Golf Polo")
    ((u'2013-07-25 00:00:00.0', -1079.73), u"2013-07-25 00:00:00.0,1079.73,Under Armour Girls' Toddler Spine Surge Runni")
    ((u'2013-07-25 00:00:00.0', -599.99), u'2013-07-25 00:00:00.0,599.99,Bowflex SelectTech 1090 Dumbbells')
```

Now , for each tuple we have to choose the part corresponding to the value. It means, taking this tuple as an example:

```python

    (u'2013-07-25 00:00:00.0', -5599.72), u'2013-07-25 00:00:00.0,5599.72,Field & Stream Sportsman 16 Gun Fire Safe')


we have to choose the part of the value: u'2013-07-25 00:00:00.0,5599.72,Field & Stream Sportsman 16 Gun Fire Safe')

    dailyRevenuePerProductName = dailyRevenuePerProductSorted.map(lambda r: r[1])
    >>> for i  in dailyRevenuePerProductName.take(10): print(i)
        ... 
        2013-07-25 00:00:00.0,5599.72,Field & Stream Sportsman 16 Gun Fire Safe
        2013-07-25 00:00:00.0,5099.49,Nike Men's Free 5.0+ Running Shoe
        2013-07-25 00:00:00.0,4499.7,Diamondback Women's Serene Classic Comfort Bi
        2013-07-25 00:00:00.0,3359.44,Perfect Fitness Perfect Rip Deck
        2013-07-25 00:00:00.0,2999.85,Pelican Sunstream 100 Kayak
        2013-07-25 00:00:00.0,2798.88,O'Brien Men's Neoprene Life Vest
        2013-07-25 00:00:00.0,1949.85,Nike Men's CJ Elite 2 TD Football Cleat
        2013-07-25 00:00:00.0,1650.0,Nike Men's Dri-FIT Victory Golf Polo
        2013-07-25 00:00:00.0,1079.73,Under Armour Girls' Toddler Spine Surge Runni
        2013-07-25 00:00:00.0,599.99,Bowflex SelectTech 1090 Dumbbells
```


10. Save final output into HDFS in avro file format as well as text file format. We save it as text file: 
        ```python

        dailyRevenuePerProductName.saveAsTextFile("/user/carlos_sanchez/daily_revenue_txt_python")
        ```

As we can see, the file has been saved correctly:
        ```python

        [carlos_sanchez@gw03 ~]$ hadoop fs -ls /user/carlos_sanchez/daily_revenue_txt_python

        Found 7 items
        -rw-r--r--   2 carlos_sanchez hdfs          0 2020-07-13 06:07 /user/carlos_sanchez/daily_revenue_txt_python/_SUCCESS
        -rw-r--r--   2 carlos_sanchez hdfs      94736 2020-07-13 06:07 /user/carlos_sanchez/daily_revenue_txt_python/part-00000
        -rw-r--r--   2 carlos_sanchez hdfs     116692 2020-07-13 06:07 /user/carlos_sanchez/daily_revenue_txt_python/part-00001
        -rw-r--r--   2 carlos_sanchez hdfs     131530 2020-07-13 06:07 /user/carlos_sanchez/daily_revenue_txt_python/part-00002
        -rw-r--r--   2 carlos_sanchez hdfs     103278 2020-07-13 06:07 /user/carlos_sanchez/daily_revenue_txt_python/part-00003
        -rw-r--r--   2 carlos_sanchez hdfs      73685 2020-07-13 06:07 /user/carlos_sanchez/daily_revenue_txt_python/part-00004
        -rw-r--r--   2 carlos_sanchez hdfs      83970 2020-07-13 06:07 /user/carlos_sanchez/daily_revenue_txt_python/part-00005
        ```

        (we see the file - just for txt format)
```python

        [carlos_sanchez@gw03 ~]$ hadoop fs -tail /user/carlos_sanchez/daily_revenue_txt_python/part-00000
        2013-09-18 00:00:00.0,44.0,Nike Dri-FIT Crew Sock 6 Pack
        2013-09-18 00:00:00.0,39.98,Top Flite Women's 2014 XL Hybrid
        2013-09-19 00:00:00.0,12399.38,Field & Stream Sportsman 16 Gun Fire Safe
        2013-09-19 00:00:00.0,7198.8,Perfect Fitness Perfect Rip Deck
        2013-09-19 00:00:00.0,5399.64,Diamondback Women's Serene Classic Comfort Bi
        2013-09-19 00:00:00.0,4999.5,Nike Men's Free 5.0+ Running Shoe
        2013-09-19 00:00:00.0,4939.62,Nike Men's CJ Elite 2 TD Football Cleat
        2013-09-19 00:00:00.0,4399.78,Pelican Sunstream 100 Kayak
        2013-09-19 00:00:00.0,4150.0,Nike Men's Dri-FIT Victory Golf Polo
        2013-09-19 00:00:00.0,3598.56,O'Brien Men's Neoprene Life Vest
        2013-09-19 00:00:00.0,1239.69,Under Armour Girls' Toddler Spine Surge Runni
        2013-09-19 00:00:00.0,499.95,Merrell Women's Grassbow Sport Hiking Shoe
        2013-09-19 00:00:00.0,299.99,Garmin Approach S4 Golf GPS Watch
        2013-09-19 00:00:00.0,299.99,Diamondback Girls' Clarity 24 Hybrid Bike 201
        2013-09-19 00:00:00.0,259.95,Titleist Pro V1x High Numbers Personalized Go


                Found 7 items
        -rw-r--r--   2 carlos_sanchez hdfs          0 2020-07-13 06:07 /user/carlos_sanchez/daily_revenue_txt_python/_SUCCESS
        -rw-r--r--   2 carlos_sanchez hdfs      94736 2020-07-13 06:07 /user/carlos_sanchez/daily_revenue_txt_python/part-00000
        -rw-r--r--   2 carlos_sanchez hdfs     116692 2020-07-13 06:07 /user/carlos_sanchez/daily_revenue_txt_python/part-00001
        -rw-r--r--   2 carlos_sanchez hdfs     131530 2020-07-13 06:07 /user/carlos_sanchez/daily_revenue_txt_python/part-00002
        -rw-r--r--   2 carlos_sanchez hdfs     103278 2020-07-13 06:07 /user/carlos_sanchez/daily_revenue_txt_python/part-00003
        -rw-r--r--   2 carlos_sanchez hdfs      73685 2020-07-13 06:07 /user/carlos_sanchez/daily_revenue_txt_python/part-00004
        -rw-r--r--   2 carlos_sanchez hdfs      83970 2020-07-13 06:07 /user/carlos_sanchez/daily_revenue_txt_python/part-00005
```


As we can see, we have 6 files. If the problem mentions we have to save it in two files, I have to use coalesce:

```python     
        dailyRevenuePerProductName.coalesce(2).saveAsTextFile("/user/carlos_sanchez/daily_revenue_txt_python")
```
If we want to delete the file:
```python

        [carlos_sanchez@gw03 ~]$ hadoop fs -rm -R /user/carlos_sanchez/daily_revenue_txt_python
        20/07/13 06:24:58 INFO fs.TrashPolicyDefault: Moved: 'hdfs://nn01.itversity.com:8020/user/carlos_sanchez/daily_revenue_txt_python' to trash at: hdfs://nn01.itversity.com:8020/user/carlos_sanchez/.Trash/Current/user/carlos_sanchez/daily_revenue_txt_python
```
In txt format we don't have metadata, but in avro we do have it: 
```python

        dailyRevenuePerProductName = dailyRevenuePerProductJoin. \
        map(lambda t: ((t[1][0][0], -t[1][0][1]),(t[1][0][0] ,round(t[1][0][1],2),t[1][1]))
        >>> for i in dailyRevenuePerProductName.take(10):print(i)
        ... 
        ((u'2013-11-02 00:00:00.0', -79.99), (u'2013-11-02 00:00:00.0', 79.99,'Elevation Training Mask 2.0' ))
        ...
```
Now that we have that key, we can sort by ascending order by date and descending order by revenue.
Once we have ordered the data, we can consider delete the key and use map to take the value  ---> (u'2013-11-02 00:00:00.0', 79.99,'Elevation Training Mask 2.0' ) and then, create an RDD of a tuple of three elements. After, we can convert it to detaframe
```python

        dailyRevenuePerProductNameDF = dailyRevenuePerProductName. \
        coalesce(2). \
        toDF(schema=["order_date", "revenue_per_product", "product_name"])

        dailyRevenuePerProductNameDF. \
        save("/user/dgadiraju/daily_revenue_avro_python", "com.databricks.spark.avro")
```
We can validate the content that we have saved properly by doing:
```python        
        sqlContext.load("/user/dgadiraju/daily_revenue_avro_python", "com.databricks.spark.avro").show()
```
With "dailyRevenuePerProductNameDF.show()" we can see the content of the dataframe



  - HDFS location - avro format /user/carlos_sanchez/daily_revenue_avro_python

  The first step is to convert the data to dataframe and then, use the mathods available in dataframe to convert to avro format. Spark supports orc and parquet formats but not avro, so we need to convert it use a plugin from databricks. If in the problem, we are given the jar we have to use, we have to launch Spark in the next way:

```python
            pyspark --master yarn \
            --conf spark.ui.port=12890 \
            --num-executors 2 \
            --executor-memory 512M \
            --jars <PATH_TO_JAR>
```
  or, if we are not said anything, we will use packages and download the packages we need:
```python

            --packages com.databricks:spark-avro_2.10:2.0.1
```


  If we preview the data
```python

            >>> for i in dailyRevenuePerProductJoin.take(10):print(i)
            ... 

            (24, ((u'2013-11-02 00:00:00.0', 79.99), 'Elevation Training Mask 2.0'))
            (24, ((u'2013-11-19 00:00:00.0', 239.97), 'Elevation Training Mask 2.0'))
            (24, ((u'2013-08-26 00:00:00.0', 239.97), 'Elevation Training Mask 2.0'))
            (24, ((u'2013-12-18 00:00:00.0', 399.95), 'Elevation Training Mask 2.0'))
            (24, ((u'2013-10-12 00:00:00.0', 239.97), 'Elevation Training Mask 2.0'))
            (24, ((u'2014-03-20 00:00:00.0', 159.98), 'Elevation Training Mask 2.0'))
            (24, ((u'2014-01-26 00:00:00.0', 239.97), 'Elevation Training Mask 2.0'))
            (24, ((u'2014-07-17 00:00:00.0', 79.99), 'Elevation Training Mask 2.0'))
            (24, ((u'2014-02-14 00:00:00.0', 159.98), 'Elevation Training Mask 2.0'))
            (24, ((u'2014-07-13 00:00:00.0', 319.96), 'Elevation Training Mask 2.0'))
```
    - HDFS location - text format /user/carlos_sanchez/daily_revenue_txt_python

11. Copy both from HDFS to local file system
    - /home/carlos_sanchez/daily_revenue_python 

    Firstly, we have to make sure we create the correct file structure:
```python
    mkdir -p /home/carlos_sanchez/daily_revenue_python
    cd /home/carlos_sanchez/daily_revenue_python
```
    Now, to copy to local file system we can use
        - hadoop fs -get ...
        - hadoop fs -copyToLocal ...

    The first argument is the source file path and the second argument corresponds to the destination location
        For txt files
```python

    hadoop fs -get /user/carlos_sanchez/daily_revenue_txt_python /home/carlos_sanchez/daily_revenue_python/daily_revenue_txt_python
    or 
    hadoop fs -get /user/carlos_sanchez/daily_revenue_txt_python /home/carlos_sanchez/daily_revenue_python/.
```

  

  For avro files:
```python

    hadoop fs -get /user/carlos_sanchez/daily_revenue_avro_python /home/carlos_sanchez/daily_revenue_python/daily_revenue_avro_python
    or 
    hadoop fs -get /user/carlos_sanchez/daily_revenue_txt_python /home/carlos_sanchez/daily_revenue_python/.
```
    Now, by doing (to list all the files in the directory):
```python

    ls -ltr daily_revenue_txt_python/
```
we have to use the resources optimally. Then we have to read orders and order_items and join them, after filtering completed and closed orders. Then we will have to get daily revenue per productId. After, we read produts data from the local file system and then I can join the data.




## How to deploy a spark project

We create the next data structure and we edit a file:
```python

[carlos_sanchez@gw03 retail]$ mkdir -p src/main/python
[carlos_sanchez@gw03 retail]$ cd src/main/python/
[carlos_sanchez@gw03 python]$ vi DailyRevenuePerProduct.py
```

```python

from pyspark import SparkConf, SparkContext

conf = SparkConf(). \
setAppName("Daily Revenue Per Product"). \
setMaster("yarn-client")
sc = SparkContext(conf=conf)
orderItems = sc.textFile("/public/retail_db/order_items")
for i in orderItems.take(10): print(i)
```

Once we have created the program, with the py extension, we have to ship the code to the cluster:
We can use scp, for example, to copy the file to the cluster where you want to run the code (as python is an interpreted programming language) - There is no source code.
To run the code we have to use the spark-submit (this have to be run where the code is)
```python
[carlos_sanchez@gw03 python]$ spark-submit \
> --master yarn \
> --conf spark.ui.port=12789 \
> --num-executors 2 \
> --executor-memory 512M \
> src/main/python/DailyRevenuePerProduct.py
```
> 
Multiple versions of Spark are installed but SPARK_MAJOR_VERSION is not set
Spark1 will be picked by default spark.yarn.driver.memoryOverhead is set but does not apply in client mode.

## SPARK-SQL AND HIVE 
To launch spark-sql:
```python
spark-sql --master yarn --conf spark.ui.port=12567
```
and the spark-sql shell will be launched

To launch Hive we can do:
```python

[carlos_sanchez@gw03 ~]$ hive

Logging initialized using configuration in file:/etc/hive/2.6.5.0-292/0/hive-log4j.properties
hive (default)> 
```

In Hive, each query will be compiled to map reduce framework. In Spark-SQL, the queries will be compiled to Spark framework.
In both cases, they have to read metadata. In the case of Hive is the Hive Metastore. Whatever we use to check the syntax of tables, columns...they are checked the same way, independently of what you use, whether spark-shell or hive. For that, reason, if you know how Hive works, you won't have any problem as for other similar tools like Impala.


**Problem**

```python
create database carlos_sanchez_retail_db_txt;

use carlos_sanchez_retail_db_txt;
OK
Time taken: 0.675 seconds


hive (carlos_sanchez_retail_db_txt)> show tables;
OK
customers
order_items
orders

```

In hive, to manage files in the directories, we have to use:
dfs -[COMMAND].
** Example **
```python

dfs -ls
```

```python
set hive.metastore.warehouse.dir;
hive.metastore.warehouse.dir=/apps/hive/warehouse;
```

Doing this we can see the directory of all the hive databases (/apps/hive/warehouse). So doing this:
```python
dfs -ls /apps/hive/warehouse;
```
we can see all the applications which have been created
```python
hive (carlos_sanchez_retail_db_txt)> dfs -ls /apps/hive/warehouse/carlos_sanchez_retail_db_txt.db;
Found 3 items
drwxrwxrwx   - carlos_sanchez hdfs          0 2020-04-16 10:19 /apps/hive/warehouse/carlos_sanchez_retail_db_txt.db/customers
drwxrwxrwx   - carlos_sanchez hdfs          0 2020-04-16 04:56 /apps/hive/warehouse/carlos_sanchez_retail_db_txt.db/order_items
drwxrwxrwx   - carlos_sanchez hdfs          0 2020-04-15 15:06 /apps/hive/warehouse/carlos_sanchez_retail_db_txt.db/orders
```

Create orders and order_items.
1. First, we will have to see the format of the tables
We go to the orders location:
```python
    [carlos_sanchez@gw03 ~]$ cd /data/retail_db/orders
    [carlos_sanchez@gw03 orders]$ ls
    part-00000
```

    And we see what the file looks like:
```python
    view part-00000
```

    As we can see, the file format is of type text:

```python    
    1,2013-07-25 00:00:00.0,11599,CLOSED
    2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT
    3,2013-07-25 00:00:00.0,12111,COMPLETE
    4,2013-07-25 00:00:00.0,8827,CLOSED
    5,2013-07-25 00:00:00.0,11318,COMPLETE
```
    However, we will be given the type of the file in the problem statement. The aim if to store what we have in the files in the order and order_items tables.
    Now we have to create the tables taking into account the column table names. For that, we have to take a look to the database model.

2. Create tables:

```python
    create table orders(
        order_id int,
        order_date string, ---> date will be encoded as type string
        order_customer_id int,
        order_status string
    ) row format delimited files terminated by ","
    stored as textfile ;


    create table orders(
        order_id int,
        order_date string,
        order_customer_id int,
        order_status string
    ) row format delimited fields terminated by ","
    stored as textfile ;


    "Hive language manual" ---> if we want to search information about Hive
```
3. Load data from local file system into the hive table:
    
```python
    hive (carlos_sanchez_retail_db_txt)> load data local inpath '/data/retail_db/orders' into table orders;
        Loading data to table carlos_sanchez_retail_db_txt.orders
        Table carlos_sanchez_retail_db_txt.orders stats: [numFiles=1, numRows=0, totalSize=2999944, rawDataSize=0]
        OK
        Time taken: 4.066 seconds
```

In case we wanted to overwrite we should have to:
```python
    load data local inpath '/data/retaild_db/orders' overwrite into table orders;
```
In case we wanted to load data from hdfs we should have to:
```python
        load data inpath '[HDFS PATH TO FILES]' 
```
The data loaded will be in that location
```python

        hive (carlos_sanchez_retail_db_txt)> dfs -ls /apps/hive/warehouse/carlos_sanchez_retail_db_txt.db/orders;
        Found 1 items
        -rwxrwxrwx   2 carlos_sanchez hdfs    2999944 2020-07-15 11:10 /apps/hive/warehouse/carlos_sanchez_retail_db_txt.db/orders/part-00000

        hive (carlos_sanchez_retail_db_txt)> select * from orders limit 10;
        OK
        1   2013-07-25 00:00:00.0   11599   CLOSED
        2   2013-07-25 00:00:00.0   256 PENDING_PAYMENT
        3   2013-07-25 00:00:00.0   12111   COMPLETE
        4   2013-07-25 00:00:00.0   8827    CLOSED
        5   2013-07-25 00:00:00.0   11318   COMPLETE
        6   2013-07-25 00:00:00.0   7130    COMPLETE
        7   2013-07-25 00:00:00.0   4530    COMPLETE
        8   2013-07-25 00:00:00.0   2911    PROCESSING
        9   2013-07-25 00:00:00.0   5657    PENDING_PAYMENT
        10  2013-07-25 00:00:00.0   5648    PENDING_PAYME

```

4.  Now we will create the table order_items:
```python

    create table order_items(
        order_item_id int,
        order_item_order_id int,
        order_item_product_id int,
        order_item_quantity int, 
        order_item_subtotal float,
        order_item_product_price float
    ) row format delimited fields terminated by ","
    stored as textfile ;


    hive (carlos_sanchez_retail_db_txt)> load data local inpath '/data/retail_db/order_items' into table order_items;
    Loading data to table carlos_sanchez_retail_db_txt.order_items
    Table carlos_sanchez_retail_db_txt.order_items stats: [numFiles=1, numRows=0, totalSize=5408880, rawDataSize=0]
    OK
    Time taken: 1.64 seconds

```
## STORE FILE IN ORC FORMAT 
1. Create the database carlos_sanchez_retail_db_orc;
```python  
    create database carlos_sanchez_db_orc;
    use carlos_sanchez_db_orc;
```

2. Create order and orderItems with file format as ORC
```python

    create table orders(
      order_id int,
      order_date string,
      order_customer_id int,
      order_status string
    ) stored as orc ;

    create table order_items(
      order_item_id int,
      order_item_order_id int,
      order_item_product_id int,
      order_item_quantity int, 
      order_item_subtotal float,
      order_item_product_price float
    ) stored as orc;
```
describe [table name] will tell the name and features of the columns

To have a more extensive description about a table, we include "formatted" option.
```
    hive (carlos_sanchez_db_orc)> describe formatted orders; 
        OK
        # col_name              data_type               comment             
                 
        order_id                int                                         
        order_date              string                                      
        order_customer_id       int                                         
        order_status            string                                      
                 
        # Detailed Table Information         
        Database:               carlos_sanchez_db_orc    
        Owner:                  carlos_sanchez           
        CreateTime:             Wed Jul 15 12:17:40 EDT 2020     
        LastAccessTime:         UNKNOWN                  
        Protect Mode:           None                     
        Retention:              0                        
        Location:               hdfs://nn01.itversity.com:8020/apps/hive/warehouse/carlos_sanchez_db_orc.db/orders   
        Table Type:             MANAGED_TABLE            
        Table Parameters:        
            COLUMN_STATS_ACCURATE   {\"BASIC_STATS\":\"true\"}
            numFiles                0                   
            numRows                 0                   
            rawDataSize             0                   
            totalSize               0                   
            transient_lastDdlTime   1594829860          
                 
        # Storage Information        
        SerDe Library:          org.apache.hadoop.hive.ql.io.orc.OrcSerde    
        InputFormat:            org.apache.hadoop.hive.ql.io.orc.OrcInputFormat  
        OutputFormat:           org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat     
        Compressed:             No                       
        Num Buckets:            -1                       
        Bucket Columns:         []                       
        Sort Columns:           []                       
        Storage Desc Params:         
            serialization.format    1                   
        Time taken: 0.611 seconds, Fetched: 34 row(s)


```
3. Insert data into tables:
    As our source data is text file format, we need to run insert command to convert data to ORC and store into the tables in new database

    If we have to load data from local that does not match with the table d estination of our data, we need to have a staging table (some kind of a preparation table).
    This table will have the same columns that the origin file (we will perform a "load into...") and then, from the staging table, we will perform an "insert into" statement to insert into that table the data we want.
```python

    hive (carlos_sanchez_db_orc)> insert into orders select * from carlos_sanchez_retail_db_txt.orders;
        Query ID = carlos_sanchez_20200715125143_8ca2b9a1-e7df-41bc-b9bf-8be873ea74b1
        Total jobs = 1
        Launching Job 1 out of 1
        Number of reduce tasks is set to 0 since there's no reduce operator
        Starting Job = job_1589064448439_25395, Tracking URL = http://rm01.itversity.com:19088/proxy/application_1589064448439_25395/
        Kill Command = /usr/hdp/2.6.5.0-292/hadoop/bin/hadoop job  -kill job_1589064448439_25395
        Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 0
        2020-07-15 12:51:55,252 Stage-1 map = 0%,  reduce = 0%
        2020-07-15 12:52:00,596 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 3.59 sec
        MapReduce Total cumulative CPU time: 3 seconds 590 msec
        Ended Job = job_1589064448439_25395
        Stage-4 is selected by condition resolver.
        Stage-3 is filtered out by condition resolver.
        Stage-5 is filtered out by condition resolver.
        Moving data to directory hdfs://nn01.itversity.com:8020/apps/hive/warehouse/carlos_sanchez_db_orc.db/orders/.hive-staging_hive_2020-07-15_12-51-43_718_4910822439187103942-1/-ext-10000
        Loading data to table carlos_sanchez_db_orc.orders
        Table carlos_sanchez_db_orc.orders stats: [numFiles=1, numRows=68883, totalSize=163115, rawDataSize=14189898]
        MapReduce Jobs Launched: 
        Stage-Stage-1: Map: 1   Cumulative CPU: 3.59 sec   HDFS Read: 3004852 HDFS Write: 163208 SUCCESS
        Total MapReduce CPU Time Spent: 3 seconds 590 msec
        OK
        Time taken: 20.198 seconds

```

And we do the same with order_items:
```python
        insert into order_items select * from carlos_sanchez_retail_db_txt.order_items;
        hive (carlos_sanchez_db_orc)> insert into order_items select * from carlos_sanchez_retail_db_txt.order_items;
            Query ID = carlos_sanchez_20200715130134_b234e19b-8952-4227-83e4-68276db213b3
            Total jobs = 1
            Launching Job 1 out of 1
            Number of reduce tasks is set to 0 since there's no reduce operator
            Starting Job = job_1589064448439_25396, Tracking URL = http://rm01.itversity.com:19088/proxy/application_1589064448439_25396/
            Kill Command = /usr/hdp/2.6.5.0-292/hadoop/bin/hadoop job  -kill job_1589064448439_25396
            Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 0
            2020-07-15 13:01:46,357 Stage-1 map = 0%,  reduce = 0%
            2020-07-15 13:01:51,671 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 3.62 sec
            MapReduce Total cumulative CPU time: 3 seconds 620 msec
            Ended Job = job_1589064448439_25396
            Stage-4 is selected by condition resolver.
            Stage-3 is filtered out by condition resolver.
            Stage-5 is filtered out by condition resolver.
            Moving data to directory hdfs://nn01.itversity.com:8020/apps/hive/warehouse/carlos_sanchez_db_orc.db/order_items/.hive-staging_hive_2020-07-15_13-01-34_017_5652849594483796431-1/-ext-10000
            Loading data to table carlos_sanchez_db_orc.order_items
            Table carlos_sanchez_db_orc.order_items stats: [numFiles=1, numRows=172198, totalSize=742945, rawDataSize=4132752]
            MapReduce Jobs Launched: 
            Stage-Stage-1: Map: 1   Cumulative CPU: 3.62 sec   HDFS Read: 5414805 HDFS Write: 743043 SUCCESS
            Total MapReduce CPU Time Spent: 3 seconds 620 msec
            OK
            Time taken: 21.125 seconds
```

If I try to search where has it been create:
```python

            hive (carlos_sanchez_db_orc)> dfs -ls hdfs://nn01.itversity.com:8020/apps/hive/warehouse/carlos_sanchez_db_orc.db/order_items;
            Found 1 items
            -rwxrwxrwx   2 carlos_sanchez hdfs     742945 2020-07-15 13:01 hdfs://nn01.itversity.com:8020/apps/hive/warehouse/carlos_sanchez_db_orc.db/order_items/000000_0
            hive (carlos_sanchez_db_orc)> 
```
ORC is not a readable format, so what we can do is to use the "select" statement in spite of the "cat" command so as to show the content of the table



## RUNNING SQL/HIVE COMMANDS USING SPARK 
```python

    [carlos_sanchez@gw03 ~]$ pyspark --master yarn --conf spark.ui.port=12562 --executor-memory 2G --num-executors 1
    Multiple versions of Spark are installed but SPARK_MAJOR_VERSION is not set
    Spark1 will be picked by default
    Python 2.7.5 (default, Aug  7 2019, 00:51:29) 
    [GCC 4.8.5 20150623 (Red Hat 4.8.5-39)] on linux2
    Type "help", "copyright", "credits" or "license" for more information.
    spark.yarn.driver.memoryOverhead is set but does not apply in client mode.
    Welcome to
          ____              __
         / __/__  ___ _____/ /__
        _\ \/ _ \/ _ `/ __/  '_/
       /__ / .__/\_,_/_/ /_/\_\   version 1.6.3
          /_/

    Using Python version 2.7.5 (default, Aug  7 2019 00:51:29)
    SparkContext available as sc, HiveContext available as sqlContext.


    sqlContext.sql("[HIVE STATEMENT]")
```
Example:
```python
    >>> sqlContext.sql("use carlos_sanchez_retail_db_txt")
        DataFrame[result: string]
```
As we can see, the  result is of type "DataFrame", so to show any table we need to use the "show" statement

```python

    >>> sqlContext.sql("show tables").show()
        +-----------+-----------+
        |  tableName|isTemporary|
        +-----------+-----------+
        |order_items|      false|
        |     orders|      false|
        +-----------+-----------+
```
When I want to get any metadata from a table, I have to use:

```python
        >>> sqlContext.sql("describe formatted orders").show()
            +--------------------+
            |              result|
            +--------------------+
            |# col_name       ...|
            |                        |
            |order_id         ...|
            |order_date       ...|
            |order_customer_id...|
            |order_status     ...|
            |                        |
            |# Detailed Table ...|
            |Database:        ...|
            |Owner:           ...|
            |CreateTime:      ...|
            |LastAccessTime:  ...|
            |Protect Mode:    ...|
            |Retention:       ...|
            |Location:        ...|
            |Table Type:      ...|
            |Table Parameters:...|
            |   numFiles        ...|
            |   numRows         ...|
            |   rawDataSize     ...|
            +--------------------+
            only showing top 20 rows

```
As we can see, we are not showing all data, so what we have to do is:

```python   
    >>> for i in sqlContext.sql("describe formatted orders").collect():print(i)
    ... 
    Row(result=u'# col_name            \tdata_type           \tcomment             ')
    Row(result=u'\t \t ')
    Row(result=u'order_id            \tint                 \t                    ')
    Row(result=u'order_date          \tstring              \t                    ')
    Row(result=u'order_customer_id   \tint                 \t                    ')
    Row(result=u'order_status        \tstring              \t                    ')
    Row(result=u'\t \t ')
    Row(result=u'# Detailed Table Information\t \t ')
    Row(result=u'Database:           \tcarlos_sanchez_retail_db_txt\t ')
    Row(result=u'Owner:              \tcarlos_sanchez      \t ')
    Row(result=u'CreateTime:         \tWed Jul 15 10:29:50 EDT 2020\t ')
    Row(result=u'LastAccessTime:     \tUNKNOWN             \t ')
    Row(result=u'Protect Mode:       \tNone                \t ')
    Row(result=u'Retention:          \t0                   \t ')
    Row(result=u'Location:           \thdfs://nn01.itversity.com:8020/apps/hive/warehouse/carlos_sanchez_retail_db_txt.db/orders\t ')
    Row(result=u'Table Type:         \tMANAGED_TABLE       \t ')
    Row(result=u'Table Parameters:\t \t ')
    Row(result=u'\tnumFiles            \t1                   ')
    Row(result=u'\tnumRows             \t0                   ')
    Row(result=u'\trawDataSize         \t0                   ')
    Row(result=u'\ttotalSize           \t2999944             ')
    Row(result=u'\ttransient_lastDdlTime\t1594825812          ')
    Row(result=u'\t \t ')
    Row(result=u'# Storage Information\t \t ')
    Row(result=u'SerDe Library:      \torg.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\t ')
    Row(result=u'InputFormat:        \torg.apache.hadoop.mapred.TextInputFormat\t ')
    Row(result=u'OutputFormat:       \torg.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\t ')
    Row(result=u'Compressed:         \tNo                  \t ')
    Row(result=u'Num Buckets:        \t-1                  \t ')
    Row(result=u'Bucket Columns:     \t[]                  \t ')
    Row(result=u'Sort Columns:       \t[]                  \t ')
    Row(result=u'Storage Desc Params:\t \t ')
    Row(result=u'\tfield.delim         \t,                   ')
    Row(result=u'\tserialization.format\t,                   ')
```
## HIVE FUNCTIONS 
```python

    [carlos_sanchez@gw03 ~]$ hive


        Logging initialized using configuration in file:/etc/hive/2.6.5.0-292/0/hive-log4j.properties
        hive (default)> 
                      > show functions;
        OK
        !
        !=
        %
        &
        *
        +
        -
```

To show the describe function of a certain function we do:
```python

        hive (default)> describe function length;
        OK
        length(str | binary) - Returns the length of str or number of bytes in binary data
        Time taken: 0.016 seconds, Fetched: 1 row(s)
        hive (default)> 

        hive (default)> select length('Hello world');
        OK
        11
        Time taken: 1.199 seconds, Fetched: 1 row(s)
```
## STRING MANIPULATION 
  1. we create the customers table:

```python

            hive (default)> use carlos_sanchez_retail_db_txt;
            OK
            Time taken: 2.301 seconds
            hive (carlos_sanchez_retail_db_txt)> create table customers(
                                               > customer_id int,
                                               > customer_fname varchar(45),
                                               > customer_lname varchar(45),
                                               > customer_email varchar(45),
                                               > customer_password varchar(45),
                                               > customer_street varchar(255),
                                               > customer_city varchar(45),
                                               > customer_state varchar(45),
                                               > customer_zipcode varchar(45)) 
                                               > row format delimited fields terminated by ',' stored as textfile;
            OK
            Time taken: 0.479 seconds
            hive (carlos_sanchez_retail_db_txt)> load data local inpath '/data/retail_db/customers' into table customers;
            Loading data to table carlos_sanchez_retail_db_txt.customers
            Table carlos_sanchez_retail_db_txt.customers stats: [numFiles=1, numRows=0, totalSize=953719, rawDataSize=0]
            OK
            Time taken: 1.12 seconds
            hive (carlos_sanchez_retail_db_txt)> 

```

| Important functions* |
| -------------------- |
| instr                |
| like                 |
| rlike                |
| length               |
| lcase or lower       |
| ucase or upper       |
| trim, ltrim, rtrim   | ---> eliminate space characters 
|lpad, rpad            |
| split                |
| initcap              |-> make words starting with capital letters
| cast ->              |


To know how a function works: --> describe function [NAME_OF_THE_FUNCTION];

### Important functions
```python

    hive (default)> describe function substr;
    OK
    substr(str, pos[, len]) - returns the substring of str that starts at pos and is of length len orsubstr(bin, pos[, len]) - returns the slice of byte array that starts at pos and is of length len
    Time taken: 2.098 seconds, Fetched: 1 row(s)
    hive (default)> [carlos_sanchez@gw03 ~]$ 

    hive (default)> select substr('Hello world, How are you',14);
    OK
    How are you
    Time taken: 4.364 seconds, Fetched: 1 row(s)

    hive (default)> select substr('Hello world, How are you',7,5);
    OK
    world
    Time taken: 0.313 seconds, Fetched: 1 row(s)

    hive (default)> select substr('Hello world, How are you',-3);
    OK
    you
    Time taken: 0.306 seconds, Fetched: 1 row(s)
```

```python

    Take the position of a string:

    hive (default)> select instr('Hello world, How are you',' ');
    OK
    6
    Time taken: 0.46 seconds, Fetched: 1 row(s)
    hive (default)> 

    hive (default)> describe function like;
    OK
    like(str, pattern) - Checks if str matches pattern
    Time taken: 0.395 seconds, Fetched: 1 row(s)
    hive (default)> 



    hive (default)> select "Hello worldd, how are you" like 'Hello%':              
    OK
    true
    Time taken: 0.333 seconds, Fetched: 1 row(s)

    hive (default)> select lpad(2,2,'0');
    OK
    02

    > select split("Hello world, how are you", ' ');
    OK
    ["Hello","world,","how","are","you"]


    hive (default)> select index(split("Hello world, how are you", ' '),4);
    OK
    you
```
### DATE MANIPULATION 

    + current_date
    + current_timestamp
    + date_add
    + date_format
    + date_sub
    + datediff
    + day
    + dayofmonth
    + to_date
    + to_unix_timestamp
    + to_utc_timestamp
    + from_unixtime
    + from_utc_timestamp
    + minute
    + month
    + months_between
    + next_day

```python


    hive (default)> select current_date;
    OK
    2020-07-16
    Time taken: 0.405 seconds, Fetched: 1 row(s)


    hive (default)> select current_timestamp;
    OK
    2020-07-16 04:38:03.516
    Time taken: 0.365 seconds, Fetched: 1 row(s)


    hive (default)> select current_date;
    OK
    2020-07-16
    Time taken: 0.405 seconds, Fetched: 1 row(s)
    hive (default)> select current_timestamp;
    OK
    2020-07-16 04:38:03.516
    Time taken: 0.365 seconds, Fetched: 1 row(s)
    hive (default)> select date_format(current_date, 'y');
    OK
    2020
    Time taken: 0.337 seconds, Fetched: 1 row(s)
    hive (default)> select date_format(current_date, 'd');
    OK
    16
    Time taken: 0.33 seconds, Fetched: 1 row(s)
    hive (default)> select date_format(current_date, 'D');
    OK
    198
    Time taken: 0.443 seconds, Fetched: 1 row(s)
    hive (default)> select date_format(current_date, 'M');
    OK
    7
    Time taken: 0.379 seconds, Fetched: 1 row(s)


    select dayofyear("2017-10-09");


    hive (default)> use carlos_sanchez_retail_db_txt;
    OK
    Time taken: 2.334 seconds
    hive (carlos_sanchez_retail_db_txt)> select to_date(order_date) from orders limit 10;
    OK
    2013-07-25
    2013-07-25
    2013-07-25
    2013-07-25
    2013-07-25
    2013-07-25
    2013-07-25
    2013-07-25
    2013-07-25
    2013-07-25
    Time taken: 0.971 seconds, Fetched: 10 row(s)



    hive (carlos_sanchez_retail_db_txt)> select date_add(order_date,10) from orders limit 10;
    OK
    2013-08-04
    2013-08-04
    2013-08-04
    2013-08-04
    2013-08-04
    2013-08-04
    2013-08-04
    2013-08-04
    2013-08-04
    2013-08-04
    Time taken: 0.212 seconds, Fetched: 10 row(s)


    hive (carlos_sanchez_retail_db_txt)> select count(*), count(distinct order_status) from orders;
    Query ID = carlos_sanchez_20200716052441_4a62472a-8a91-4187-abef-6fd3e84e889e
    Total jobs = 1
    Launching Job 1 out of 1
    Number of reduce tasks determined at compile time: 1
    In order to change the average load for a reducer (in bytes):
      set hive.exec.reducers.bytes.per.reducer=<number>
    In order to limit the maximum number of reducers:
      set hive.exec.reducers.max=<number>
    In order to set a constant number of reducers:
      set mapreduce.job.reduces=<number>
    Starting Job = job_1589064448439_25562, Tracking URL = http://rm01.itversity.com:19088/proxy/application_1589064448439_25562/
    Kill Command = /usr/hdp/2.6.5.0-292/hadoop/bin/hadoop job  -kill job_1589064448439_25562
    Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
    2020-07-16 05:24:55,217 Stage-1 map = 0%,  reduce = 0%
    2020-07-16 05:25:00,494 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 2.48 sec
    2020-07-16 05:25:06,262 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 4.87 sec
    MapReduce Total cumulative CPU time: 4 seconds 870 msec
    Ended Job = job_1589064448439_25562
    MapReduce Jobs Launched: 
    Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 4.87 sec   HDFS Read: 3008673 HDFS Write: 8 SUCCESS
    Total MapReduce CPU Time Spent: 4 seconds 870 msec
    OK
    68883   9
    Time taken: 26.93 seconds, Fetched: 1 row(s)
```

## CASE AND NVL 

We can use "if" or "case" to specify conditions:
```python
    hive (carlos_sanchez_retail_db_txt)> describe function if;
        OK
        IF(expr1,expr2,expr3) - If expr1 is TRUE (expr1 <> 0 and expr1 <> NULL) then IF() returns expr2; otherwise it returns expr3. IF() returns a numeric or string value, depending on the context in which it is used.
        Time taken: 0.174 seconds, Fetched: 1 row(s)

    hive (carlos_sanchez_retail_db_txt)> describe function case;
        OK
        CASE a WHEN b THEN c [WHEN d THEN e]* [ELSE f] END - When a = b, returns c; when a = d, return e; else return f
        Time taken: 0.008 seconds, Fetched: 1 row(s)

    hive (carlos_sanchez_retail_db_txt)> select distinct(order_status) from orders;
        Query ID = carlos_sanchez_20200716053101_a9000e9b-cb6f-44d6-8050-bf220e1ced2b
        Total jobs = 1
        Launching Job 1 out of 1
        Number of reduce tasks not specified. Estimated from input data size: 1
        In order to change the average load for a reducer (in bytes):
          set hive.exec.reducers.bytes.per.reducer=<number>
        In order to limit the maximum number of reducers:
          set hive.exec.reducers.max=<number>
        In order to set a constant number of reducers:
          set mapreduce.job.reduces=<number>
        Starting Job = job_1589064448439_25566, Tracking URL = http://rm01.itversity.com:19088/proxy/application_1589064448439_25566/
        Kill Command = /usr/hdp/2.6.5.0-292/hadoop/bin/hadoop job  -kill job_1589064448439_25566
        Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
        2020-07-16 05:31:10,914 Stage-1 map = 0%,  reduce = 0%
        2020-07-16 05:31:16,129 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 2.55 sec
        2020-07-16 05:31:23,428 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 5.16 sec
        MapReduce Total cumulative CPU time: 5 seconds 160 msec
        Ended Job = job_1589064448439_25566
        MapReduce Jobs Launched: 
        Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 5.16 sec   HDFS Read: 3007851 HDFS Write: 99 SUCCESS
        Total MapReduce CPU Time Spent: 5 seconds 160 msec
        OK
        CANCELED
        CLOSED
        COMPLETE
        ON_HOLD
        PAYMENT_REVIEW
        PENDING
        PENDING_PAYMENT
        PROCESSING
        SUSPECTED_FRAUD
        Time taken: 22.983 seconds, Fetched: 9 row(s)


        hive (carlos_sanchez_retail_db_txt)> select case order_status
                                   >   when 'CLOSED' then 'No Action'
                                   >   when 'COMPLETE' then 'No Action'
                                   >   end from orders limit 10;
        OK
        No Action
        NULL
        No Action
        No Action
        No Action
        No Action
        No Action
        NULL
        NULL
        NULL
        Time taken: 0.269 seconds, Fetched: 10 row(s)

```

### NVL
        
        If a column is null then...
```python
        hive (carlos_sanchez_retail_db_txt)> select nvl(order_status, 'Status Missing') from orders limit 100;
        OK
        CLOSED
        PENDING_PAYMENT
        COMPLETE
        CLOSED
        COMPLETE
        COMPLETE
        COMPLETE
        PROCESSING
        PENDING_PAYMENT
        PENDING_PAYMENT
```
## ROW LEVEL TRANSFORMATIONS 
```python
    hive (carlos_sanchez_retail_db_txt)> select concat(substr(order_date,1,4), substr(order_date, 6,2)) from orders limit 10;
    OK
    201307
    201307
    201307
    201307
    201307
    201307
    201307
    201307
    201307
    201307


    hive (carlos_sanchez_retail_db_txt)> select cast(concat(substr(order_date,1,4), substr(order_date, 6,2))as int) from orders limit 10;
    OK
    201307
    201307
    201307
    201307
    201307
    201307
    201307
    201307
    201307
    201307
```
    This is another way to extract the information from the date the way we 
    want:
```python

    hive (carlos_sanchez_retail_db_txt)> select date_format('2013-07-25 00:00:00.0', 'YYYYMM');
    OK
    201307
    Time taken: 0.391 seconds, Fetched: 1 row(s)

```

### JOINING DATA BETWEEN MULTIPLE TABLES 

    There will be multiple items in each order, we will have a cardinality of one to many tables between orders and order_items tables.
```python
    hive (carlos_sanchez_retail_db_txt)> select o.*, c.* from orders o, customers c where o.order_customer_id = c.customer_id limit 10;
    Query ID = carlos_sanchez_20200716063355_b0f14964-070e-4948-b018-8adf77a7f932
    Total jobs = 1
    Execution log at: /tmp/carlos_sanchez/carlos_sanchez_20200716063355_b0f14964-070e-4948-b018-8adf77a7f932.log
    2020-07-16 06:34:01 Starting to launch local task to process map join;  maximum memory = 1046478848
    2020-07-16 06:34:03 Dump the side-table for tag: 1 with group count: 12435 into file: file:/tmp/carlos_sanchez/0062ce28-dd3d-4d1c-b04f-05e8d59c78cf/hive_2020-07-16_06-33-55_038_5757665311343670280-1/-local-10003/HashTable-Stage-3/MapJoin-mapfile01--.hashtable
    2020-07-16 06:34:03 Uploaded 1 File to: file:/tmp/carlos_sanchez/0062ce28-dd3d-4d1c-b04f-05e8d59c78cf/hive_2020-07-16_06-33-55_038_5757665311343670280-1/-local-10003/HashTable-Stage-3/MapJoin-mapfile01--.hashtable (1156846 bytes)
    2020-07-16 06:34:03 End of local task; Time Taken: 2.196 sec.
    Execution completed successfully
    MapredLocal task succeeded
    Launching Job 1 out of 1
    Number of reduce tasks is set to 0 since there's no reduce operator
    Starting Job = job_1589064448439_25582, Tracking URL = http://rm01.itversity.com:19088/proxy/application_1589064448439_25582/
    Kill Command = /usr/hdp/2.6.5.0-292/hadoop/bin/hadoop job  -kill job_1589064448439_25582
    Hadoop job information for Stage-3: number of mappers: 1; number of reducers: 0
    2020-07-16 06:34:13,947 Stage-3 map = 0%,  reduce = 0%
    2020-07-16 06:34:20,231 Stage-3 map = 100%,  reduce = 0%, Cumulative CPU 4.87 sec
    MapReduce Total cumulative CPU time: 4 seconds 870 msec
    Ended Job = job_1589064448439_25582
    MapReduce Jobs Launched: 
    Stage-Stage-3: Map: 1   Cumulative CPU: 4.87 sec   HDFS Read: 142126 HDFS Write: 1171 SUCCESS
    Total MapReduce CPU Time Spent: 4 seconds 870 msec
    OK
    1   2013-07-25 00:00:00.0   11599   CLOSED  11599   Mary    Malone  XXXXXXXXX   XXXXXXXXX   8708 Indian Horse Highway   Hickory NC  28601
    2   2013-07-25 00:00:00.0   256 PENDING_PAYMENT 256 David   Rodriguez   XXXXXXXXX   XXXXXXXXX   7605 Tawny Horse Falls  Chicago IL  60625
    3   2013-07-25 00:00:00.0   12111   COMPLETE    12111   Amber   Franco  XXXXXXXXX   XXXXXXXXX   8766 Clear Prairie Line Santa Cruz  CA  95060
    4   2013-07-25 00:00:00.0   8827    CLOSED  8827    Brian   Wilson  XXXXXXXXX   XXXXXXXXX   8396 High Corners   San Antonio TX  78240
    5   2013-07-25 00:00:00.0   11318   COMPLETE    11318   Mary    Henry   XXXXXXXXX   XXXXXXXXX   3047 Silent Embers Maze Caguas  PR  00725
    6   2013-07-25 00:00:00.0   7130    COMPLETE    7130    Alice   Smith   XXXXXXXXX   XXXXXXXXX   8852 Iron Port  Brooklyn    NY  11237
    7   2013-07-25 00:00:00.0   4530    COMPLETE    4530    Mary    Smith   XXXXXXXXX   XXXXXXXXX   1073 Green Leaf Green   Miami   FL  33161
    8   2013-07-25 00:00:00.0   2911    PROCESSING  2911    Mary    Smith   XXXXXXXXX   XXXXXXXXX   9166 Golden Nectar Corner   Caguas  PR  00725
    9   2013-07-25 00:00:00.0   5657    PENDING_PAYMENT 5657    Mary    James   XXXXXXXXX   XXXXXXXXX   1389 Dusty Circuit  Lakewood    OH  44107
    10  2013-07-25 00:00:00.0   5648    PENDING_PAYMENT 5648    Joshua  Smith   XXXXXXXXX   XXXXXXXXX   864 Iron Spring Stead   Memphis TN  38111
    Time taken: 27.519 seconds, Fetched: 10 row(s)
    
```
    we can do it in a more readable way:
```python

    select o.*, c.* from orders o join customers c on o.order_customer_id = c.customer_id limit 10;
```
Customers that have and may now have an order (left outer join):

```python  
    select o., c.* from customers c left outer join orders on o.order_customer_id = c.customer_id limit 10;


    hive (carlos_sanchez_retail_db_txt)> select o.*, c.* from customers c left outer join orders o on o.order_customer_id = c.customer_id limit 10;
    Query ID = carlos_sanchez_20200716064424_d8e3885f-6346-443b-a330-520ed559694a
    Total jobs = 1
    Execution log at: /tmp/carlos_sanchez/carlos_sanchez_20200716064424_d8e3885f-6346-443b-a330-520ed559694a.log
    2020-07-16 06:44:29 Starting to launch local task to process map join;  maximum memory = 1046478848
    2020-07-16 06:44:30 Dump the side-table for tag: 1 with group count: 12405 into file: file:/tmp/carlos_sanchez/0062ce28-dd3d-4d1c-b04f-05e8d59c78cf/hive_2020-07-16_06-44-24_146_9032688902326370015-1/-local-10003/HashTable-Stage-3/MapJoin-mapfile21--.hashtable
    2020-07-16 06:44:30 Uploaded 1 File to: file:/tmp/carlos_sanchez/0062ce28-dd3d-4d1c-b04f-05e8d59c78cf/hive_2020-07-16_06-44-24_146_9032688902326370015-1/-local-10003/HashTable-Stage-3/MapJoin-mapfile21--.hashtable (3012920 bytes)
    2020-07-16 06:44:30 End of local task; Time Taken: 1.523 sec.
    Execution completed successfully
    MapredLocal task succeeded
    Launching Job 1 out of 1
    Number of reduce tasks is set to 0 since there's no reduce operator
    Starting Job = job_1589064448439_25588, Tracking URL = http://rm01.itversity.com:19088/proxy/application_1589064448439_25588/
    Kill Command = /usr/hdp/2.6.5.0-292/hadoop/bin/hadoop job  -kill job_1589064448439_25588
    Hadoop job information for Stage-3: number of mappers: 1; number of reducers: 0
    2020-07-16 06:44:40,561 Stage-3 map = 0%,  reduce = 0%
    2020-07-16 06:44:45,761 Stage-3 map = 100%,  reduce = 0%, Cumulative CPU 3.38 sec
    MapReduce Total cumulative CPU time: 3 seconds 380 msec
    Ended Job = job_1589064448439_25588
    MapReduce Jobs Launched: 
    Stage-Stage-3: Map: 1   Cumulative CPU: 3.38 sec   HDFS Read: 141942 HDFS Write: 1144 SUCCESS
    Total MapReduce CPU Time Spent: 3 seconds 380 msec
    OK
    22945   2013-12-13 00:00:00.0   1   COMPLETE    1   Richard Hernandez   XXXXXXXXX   XXXXXXXXX   6303 Heather Plaza  Brownsville TX  78521
    15192   2013-10-29 00:00:00.0   2   PENDING_PAYMENT 2   Mary    Barrett XXXXXXXXX   XXXXXXXXX   9526 Noble Embers Ridge Littleton   CO  80126
    33865   2014-02-18 00:00:00.0   2   COMPLETE    2   Mary    Barrett XXXXXXXXX   XXXXXXXXX   9526 Noble Embers Ridge Littleton   CO  80126
    57963   2013-08-02 00:00:00.0   2   ON_HOLD 2   Mary    Barrett XXXXXXXXX   XXXXXXXXX   9526 Noble Embers Ridge Littleton   CO  80126
    67863   2013-11-30 00:00:00.0   2   COMPLETE    2   Mary    Barrett XXXXXXXXX   XXXXXXXXX   9526 Noble Embers Ridge Littleton   CO  80126
    22646   2013-12-11 00:00:00.0   3   COMPLETE    3   Ann Smith   XXXXXXXXX   XXXXXXXXX   3422 Blue Pioneer Bend  Caguas  PR  00725
    23662   2013-12-19 00:00:00.0   3   COMPLETE    3   Ann Smith   XXXXXXXXX   XXXXXXXXX   3422 Blue Pioneer Bend  Caguas  PR  00725
    35158   2014-02-26 00:00:00.0   3   COMPLETE    3   Ann Smith   XXXXXXXXX   XXXXXXXXX   3422 Blue Pioneer Bend  Caguas  PR  00725
    46399   2014-05-09 00:00:00.0   3   PROCESSING  3   Ann Smith   XXXXXXXXX   XXXXXXXXX   3422 Blue Pioneer Bend  Caguas  PR  00725
    56178   2014-07-15 00:00:00.0   3   PENDING 3   Ann Smith   XXXXXXXXX   XXXXXXXXX   3422 Blue Pioneer Bend  Caguas  PR  00725
    Time taken: 23.875 seconds, Fetched: 10 row(s)
```

**Problem** 
Get all the customers who do not have any corresponding orders (you want data which is in customers but not in orders)

```python
    hive (carlos_sanchez_retail_db_txt)> select count(1) from customers c left outer join orders o on o.order_customer_id = c.customer_id where o.order_customer_id is null;
    Query ID = carlos_sanchez_20200716065101_d6da3cf3-9ea4-40ac-9913-51084b539775
    Total jobs = 1
    Execution log at: /tmp/carlos_sanchez/carlos_sanchez_20200716065101_d6da3cf3-9ea4-40ac-9913-51084b539775.log
    2020-07-16 06:51:15 Starting to launch local task to process map join;  maximum memory = 1046478848
    2020-07-16 06:51:18 Dump the side-table for tag: 1 with group count: 12405 into file: file:/tmp/carlos_sanchez/0062ce28-dd3d-4d1c-b04f-05e8d59c78cf/hive_2020-07-16_06-51-01_588_707542416438151942-1/-local-10004/HashTable-Stage-2/MapJoin-mapfile31--.hashtable
    2020-07-16 06:51:18 Uploaded 1 File to: file:/tmp/carlos_sanchez/0062ce28-dd3d-4d1c-b04f-05e8d59c78cf/hive_2020-07-16_06-51-01_588_707542416438151942-1/-local-10004/HashTable-Stage-2/MapJoin-mapfile31--.hashtable (476204 bytes)
    2020-07-16 06:51:18 End of local task; Time Taken: 2.363 sec.
    Execution completed successfully
    MapredLocal task succeeded
    Launching Job 1 out of 1
    Number of reduce tasks determined at compile time: 1
    In order to change the average load for a reducer (in bytes):
      set hive.exec.reducers.bytes.per.reducer=<number>
    In order to limit the maximum number of reducers:
      set hive.exec.reducers.max=<number>
    In order to set a constant number of reducers:
      set mapreduce.job.reduces=<number>
    Starting Job = job_1589064448439_25590, Tracking URL = http://rm01.itversity.com:19088/proxy/application_1589064448439_25590/
    Kill Command = /usr/hdp/2.6.5.0-292/hadoop/bin/hadoop job  -kill job_1589064448439_25590
    Hadoop job information for Stage-2: number of mappers: 1; number of reducers: 1
    2020-07-16 06:51:28,270 Stage-2 map = 0%,  reduce = 0%
    2020-07-16 06:51:34,492 Stage-2 map = 100%,  reduce = 0%, Cumulative CPU 5.23 sec
    2020-07-16 06:51:40,708 Stage-2 map = 100%,  reduce = 100%, Cumulative CPU 8.15 sec
    MapReduce Total cumulative CPU time: 8 seconds 150 msec
    Ended Job = job_1589064448439_25590
    MapReduce Jobs Launched: 
    Stage-Stage-2: Map: 1  Reduce: 1   Cumulative CPU: 8.15 sec   HDFS Read: 969388 HDFS Write: 3 SUCCESS
    Total MapReduce CPU Time Spent: 8 seconds 150 msec
    OK
    30

```
In addition, we can write that query with a right outer join, where the parent table is on the right:
```python

    select count(1) from orders o right outer join customers c on o.order_customer_id = c.customer_id where o.order_customer_id is null;
```
With full outer join we will consider data that is just in one table but not in the other one.


### GROUP BY AND AGGREGATIONS 
```python

    hive (carlos_sanchez_retail_db_txt)> select order_status, count(1) from orders group by order_status;
    Query ID = carlos_sanchez_20200716071632_037cbf32-f852-4967-a035-d11c3a221389
    Total jobs = 1
    Launching Job 1 out of 1
    Number of reduce tasks not specified. Estimated from input data size: 1
    In order to change the average load for a reducer (in bytes):
      set hive.exec.reducers.bytes.per.reducer=<number>
    In order to limit the maximum number of reducers:
      set hive.exec.reducers.max=<number>
    In order to set a constant number of reducers:
      set mapreduce.job.reduces=<number>
    Starting Job = job_1589064448439_25599, Tracking URL = http://rm01.itversity.com:19088/proxy/application_1589064448439_25599/
    Kill Command = /usr/hdp/2.6.5.0-292/hadoop/bin/hadoop job  -kill job_1589064448439_25599
    Hadoop job information for Stage-1: number of mappers: 1; number of reducers: 1
    2020-07-16 07:16:44,579 Stage-1 map = 0%,  reduce = 0%
    2020-07-16 07:16:49,839 Stage-1 map = 100%,  reduce = 0%, Cumulative CPU 2.55 sec
    2020-07-16 07:16:55,046 Stage-1 map = 100%,  reduce = 100%, Cumulative CPU 5.15 sec
    MapReduce Total cumulative CPU time: 5 seconds 150 msec
    Ended Job = job_1589064448439_25599
    MapReduce Jobs Launched: 
    Stage-Stage-1: Map: 1  Reduce: 1   Cumulative CPU: 5.15 sec   HDFS Read: 3008587 HDFS Write: 145 SUCCESS
    Total MapReduce CPU Time Spent: 5 seconds 150 msec
    OK
    CANCELED    1428
    CLOSED  7556
    COMPLETE    22899
    ON_HOLD 3798
    PAYMENT_REVIEW  729
    PENDING 7610
    PENDING_PAYMENT 15030
    PROCESSING  8275
    SUSPECTED_FRAUD 1558
    Time taken: 24.594 seconds, Fetched: 9 row(s)
```

**Problem**
From each order I want to get  the revenue.
To do it we have to group by order_id and then get the sum of the revenues.
```python

    select o.order_id, sum(oi.order_item_subtotal) order_revenue
        from orders o join order_items oi    
        on o.order_id = oi.order_item_order_id
        group by (o.order_id);

        68865   399.9800109863281
        68866   279.97000885009766
        68868   429.9600067138672
        68869   1229.9699935913086
        68870   479.9200134277344
        68871   499.9800109863281
        68873   859.9100151062012

#Get the revenue per order and date having done more than 1000 dollars (COMPLETED and CLOSED revenues)

        select o.order_id, o.order_date, o.order_status, round(sum(oi.order_item_subtotal), 2) order_revenue
        from orders o join order_items oi
        on o.order_id = oi.order_item_order_id
        where o.order_status in ('COMPLETE', 'CLOSED')
        group by o.order_id, o.order_date, o.order_status
        having sum(oi.order_item_subtotal) >= 1000;

        60578   2013-11-13 00:00:00.0   COMPLETE    1239.81
        60609   2013-11-14 00:00:00.0   COMPLETE    1109.91
        60640   2013-11-15 00:00:00.0   COMPLETE    1279.9
        60643   2013-11-15 00:00:00.0   COMPLETE    1279.74
        60645   2013-11-16 00:00:00.0   COMPLETE    1049.93
        60656   2013-11-16 00:00:00.0   CLOSED  1059.86

```
As we can see, data is grouped by order. If we wanted to group by date:
```python

        select o.order_date,round(sum(oi.order_item_subtotal), 2) daily_revenue
        from orders o join order_items oi
        on o.order_id = oi.order_item_order_id
        where o.order_status in ('COMPLETE', 'CLOSED')
        group by o.order_date;

        2014-06-29 00:00:00.0   27772.93
        2014-06-30 00:00:00.0   36990.68
        2014-07-01 00:00:00.0   40165.66
        2014-07-02 00:00:00.0   26480.01
        2014-07-03 00:00:00.0   37756.8
        2014-07-04 00:00:00.0   25467.71
        2014-07-05 00:00:00.0   47214.68
        2014-07-06 00:00:00.0   16451.76
        2014-07-07 00:00:00.0   35441.49
        2014-07-08 00:00:00.0   50434.81
        2014-07-09 00:00:00.0   36929.91
        2014-07-10 00:00:00.0   47826.02
```
### SORTING THE DATA  

```
        select o.order_date,round(sum(oi.order_item_subtotal),2) daily_revenue
        from orders o join order_items oi
        on o.order_id = oi.order_item_order_id
        where o.order_status in ('COMPLETE', 'CLOSED')
        group by o.order_date, oi.order_item_subtotal
        having sum(oi.order_item_subtotal) >= 1000
        order by o.order_date, daily_revenue desc;

        2014-07-09 00:00:00.0   1200.0
        2014-07-09 00:00:00.0   1199.8
        2014-07-09 00:00:00.0   1199.8
        2014-07-10 00:00:00.0   7999.6
        2014-07-10 00:00:00.0   5599.72
        2014-07-10 00:00:00.0   5399.64
        2014-07-10 00:00:00.0   5069.61
        2014-07-10 00:00:00.0   3119.48
        2014-07-10 00:00:00.0   1999.8
        2014-07-10 00:00:00.0   1800.0
```

We can also use SORT BY:
```python        
        select o.order_date,round(sum(oi.order_item_subtotal),2) daily_revenue
        from orders o join order_items oi
        on o.order_id = oi.order_item_order_id
        where o.order_status in ('COMPLETE', 'CLOSED')
        group by o.order_date, oi.order_item_subtotal
        having sum(oi.order_item_subtotal) >= 1000
        sort by o.order_date, daily_revenue desc;
```
Using distribute, it will distribute the data by date in descending order by revenue ---------------------------BUSCAR INFO DE DISTRIBUTE
We can also use SORT BY:
```python

        select o.order_date,round(sum(oi.order_item_subtotal),2) daily_revenue
        from orders o join order_items oi
        on o.order_id = oi.order_item_order_id
        where o.order_status in ('COMPLETE', 'CLOSED')
        group by o.order_date, oi.order_item_subtotal
        having sum(oi.order_item_subtotal) >= 1000
        distribute by o.order_date sort by o.order_date, daily_revenue desc;

```

Using distribute is more efficient than sorting by, as it does not sort the whole data by a criteria. It assures the data will be sorted randomly on date, and then, data will be sorted by revenue (it is an efficiency issue)

### SET OPERATIONS  
        
You cannot perform a set operation between two datasets that are not of the same type:

```python
    select 1, "Hello"
    union all 
    select 2, "world" ----> if we use union all it will not sort the data

    select 1, "Hello"
    union 
    select 2, "world" -----> if we use union, in spite of union all, it will eliminate duplicates (is like distinct)
```

### ANALYTICS OPERATION  

What percentage has generated each revenue has generated in each order
```python

    hive (carlos_sanchez_retail_db_txt)> select * from order_items limit 10;
    OK
    1   1   957 1   299.98  299.98
    2   2   1073    1   199.99  199.99 ----> If we take into account the order_id (4 th column), as for order_id number = 1, this row will 
    3   2   502 5   250.0   50.0             199.99 will represent a percentage of the total sum (299.8 + 199.99 + 129.990+ 299.98). What we want is
    4   2   403 1   129.99  129.99           to have that percentage in all columns
    5   4   897 2   49.98   24.99
    6   4   365 5   299.95  59.99
    7   4   502 3   150.0   50.0
    8   4   1014    4   199.92  49.98
    9   5   957 1   299.98  299.98
    10  5   365 5   299.95  59.99

```

Having the "over" clause, we will have to delete the "group by" clause

```python
        select * from ( ----> we add that "select" statement in brackets so as to be able to select for the column alias which we have created below (order_revenue, 
                               pct_revenue, avg_revenue...
        select o.order_id, o.order_date, o.order_status, oi.order_item_subtotal, 
        round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2) order_revenue,
        oi.order_item_subtotal/round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2) pct_revenue,
        round(avg(oi.order_item_subtotal) over (partition by o.order_id), 2) avg_revenue
        from orders o join order_items oi
        on o.order_id = oi.order_item_order_id
        where o.order_status in ('COMPLETE', 'CLOSED')) q
        where order_revenue >= 1000
        order by order_date, order_revenue desc, pct_revenue;


        56094   2014-07-15 00:00:00.0   COMPLETE    129.99  1169.84 0.11111776438928749 233.97
        56094   2014-07-15 00:00:00.0   COMPLETE    199.92  1169.84 0.1708951635855718  233.97
        56094   2014-07-15 00:00:00.0   COMPLETE    199.99  1169.84 0.17095500708914388 233.97
        56094   2014-07-15 00:00:00.0   COMPLETE    239.96  1169.84 0.20512207371424057 233.97
        56094   2014-07-15 00:00:00.0   COMPLETE    399.98  1169.84 0.34191001417828776 233.97
        56062   2014-07-15 00:00:00.0   COMPLETE    129.99  1119.79 0.11608427070536803 223.96
        56062   2014-07-15 00:00:00.0   COMPLETE    199.99  1119.79 0.1785959916530457  223.96
        56062   2014-07-15 00:00:00.0   COMPLETE    239.96  1119.79 0.2142901854042876  223.96
        56062   2014-07-15 00:00:00.0   COMPLETE    249.9   1119.79 0.22316683833261985 223.96
        56062   2014-07-15 00:00:00.0   COMPLETE    299.95  1119.79 0.26786273516197795 223.96
        56100   2014-07-15 00:00:00.0   COMPLETE    59.99   1039.85 0.057691014741036495    207.97
        56100   2014-07-15 00:00:00.0   COMPLETE    129.99  1039.85 0.12500841995784398 207.97
        56100   2014-07-15 00:00:00.0   COMPLETE    199.99  1039.85 0.19232582150614425 207.97
        56100   2014-07-15 00:00:00.0   COMPLETE    249.9   1039.85 0.24032311765782027 207.97
        56100   2014-07-15 00:00:00.0   COMPLETE    399.98  1039.85 0.3846516430122885  207.97
        56346   2014-07-16 00:00:00.0   COMPLETE    199.92  1499.82 0.13329599429861272 299.96
        56346   2014-07-16 00:00:00.0   COMPLETE    199.98  1499.82 0.13333599747138927 299.96
        56346   2014-07-16 00:00:00.0   COMPLETE    199.99  1499.82 0.13334267144934997 299.96
        56346   2014-07-16 00:00:00.0   COMPLETE    399.98  1499.82 0.26668534289869994 299.96

```
### RANK FUNCTION 
There is a clause for that purpose: "rank()". Rank will always live with an "order by" clause. If we add "desc", we will create a ranking in descending order.

```python
    select * from (
    select o.order_id, o.order_date, o.order_status, oi.order_item_subtotal,
    round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2) order_revenue,
    oi.order_item_subtotal/round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2) pct_revenue,
    round(avg(oi.order_item_subtotal) over (partition by o.order_id), 2) avg_revenue,
    rank() over (partition by o.order_id order by oi.order_item_subtotal desc) rnk_revenue,
    dense_rank() over (partition by o.order_id order by oi.order_item_subtotal desc) dense_rnk_revenue,
    percent_rank() over (partition by o.order_id order by oi.order_item_subtotal desc) pct_rnk_revenue,
    row_number() over (partition by o.order_id order by oi.order_item_subtotal desc) rn_orderby_revenue,
    row_number() over (partition by o.order_id) rn_revenue
    from orders o join order_items oi
    on o.order_id = oi.order_item_order_id
    where o.order_status in ('COMPLETE', 'CLOSED')) q
    where order_revenue >= 1000
    order by order_date, order_revenue desc, rnk_revenue;
```



### WINDOWING FUNCTION  
* Lead ---> It will fetch the next record within a group  
* Lag  ---> It will fetch the previous record within a group
* First_value ---> first record within a group
* Last_value  ---> last record within a group

```python

    hive (carlos_sanchez_retail_db_txt)> 
                                   > select * from (
                                   > select o.order_id, o.order_date, o.order_status, oi.order_item_subtotal, 
                                   > round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2) order_revenue,
                                   > oi.order_item_subtotal/round(sum(oi.order_item_subtotal) over (partition by o.order_id), 2) pct_revenue,
                                   > round(avg(oi.order_item_subtotal) over (partition by o.order_id), 2) avg_revenue,
                                   > rank() over (partition by o.order_id order by oi.order_item_subtotal desc) rnk_revenue,
                                   > dense_rank() over (partition by o.order_id order by oi.order_item_subtotal desc) dense_rnk_revenue,
                                   > percent_rank() over (partition by o.order_id order by oi.order_item_subtotal desc) pct_rnk_revenue,
                                   > row_number() over (partition by o.order_id order by oi.order_item_subtotal desc) rn_orderby_revenue,
                                   > row_number() over (partition by o.order_id) rn_revenue,
                                   > lead(oi.order_item_subtotal) over (partition by o.order_id order by oi.order_item_subtotal desc) lead_order_item_subtotal,
                                   > lag(oi.order_item_subtotal) over (partition by o.order_id order by oi.order_item_subtotal desc) lag_order_item_subtotal,
                                   > first_value(oi.order_item_subtotal) over (partition by o.order_id order by oi.order_item_subtotal desc) first_order_item_subtotal,
                                   > last_value(oi.order_item_subtotal) over (partition by o.order_id order by oi.order_item_subtotal desc) last_order_item_subtotal
                                   > from orders o join order_items oi
                                   > on o.order_id = oi.order_item_order_id
                                   > where o.order_status in ('COMPLETE', 'CLOSED')) q
                                   > where order_revenue >= 1000
                                   > order by order_date, order_revenue desc, rnk_revenue;

56513   2014-07-17 00:00:00.0   COMPLETE    249.9   1139.83 0.21924321512548747 227.97  2   2   0.25    2   4   239.96  299.98  299.98  249.9
56513   2014-07-17 00:00:00.0   COMPLETE    239.96  1139.83 0.2105226276847137  227.97  3   3   0.5 3   1   199.99  249.9   299.98  239.96
56513   2014-07-17 00:00:00.0   COMPLETE    199.99  1139.83 0.1754559938702825  227.97  4   4   0.75    4   5   150.0   239.96  299.98  199.99
56513   2014-07-17 00:00:00.0   COMPLETE    150.0   1139.83 0.13159857171683498 227.97  5   5   1.0 5   2   NULL    199.99  299.98  150.0
67206   2014-07-17 00:00:00.0   COMPLETE    399.98  1129.87 0.3540053377701224  225.97  1   1   0.0 1   4   239.96  NULL    399.98  399.98
67206   2014-07-17 00:00:00.0   COMPLETE    239.96  1129.87 0.21237842115806882 225.97  2   2   0.25    2   1   239.96  399.98  399.98  239.96
67206   2014-07-17 00:00:00.0   COMPLETE    239.96  1129.87 0.21237842115806882 225.97  2   2   0.25    3   2   129.99  239.96  399.98  239.96
67206   2014-07-17 00:00:00.0   COMPLETE    129.99  1129.87 0.11504863877540254 225.97  4   3   0.75    4   5   119.98  239.96  399.98  129.99
67206   2014-07-17 00:00:00.0   COMPLETE    119.98  1129.87 0.10618921057903441 225.97  5   4   1.0 5   3   NULL    129.99  399.98  119.98

```

### CREATING DATAFRAMES AND REGISTER TEMP TABLES  
**Problem**
Get daily revenue by product considering completed and closed orders.

      + PRODUCTS have to be read from the local file system. Dataframe need to be created
      + Join ORDERS and ORDER_ITEMS
      + Filter on ORDER_STATUS

Data need to be sorted by ascending order by date and then descending order by revenue computed for each product each day. 
    - Sort data by order_date in ascending order and then daily_revenue per product in descending order

1. we need to run pyspark:
```python    
   pyspark --master yarn --conf spark.ui.port=12562 --executor-memory 2G --num-executors 1
```

2.
```python
>>> sqlContext.sql("select * from carlos_sanchez_db_orc.orders limit 10")
    DataFrame[order_id: int, order_date: string, order_customer_id: int, order_status: string]
```
    
The result of a sql query will be a dataframe. So we will use the "show" funtion to see what does the table have

```python    
    >>> sqlContext.sql("select * from carlos_sanchez_db_orc.orders").show(10)
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
```


To see the schema from the table, we use the nex command:
```python    
    >>> sqlContext.sql("select * from carlos_sanchez_db_orc.orders").printSchema()

    root
     |-- order_id: integer (nullable = true)
     |-- order_date: string (nullable = true)
     |-- order_customer_id: integer (nullable = true)
     |-- order_status: string (nullable = true)
```

Also, if we want to convert a RDD into a Dataframe, we will have to use:
```python
     from pyspark.sql import Row
     #Now, we will read the data from HDFS:
     ordersRDD = sc.textFile("/public/retail_db/orders")

     # Now we can preview the data to check data has been loaded properly. 
     >>> for i in ordersRDD.take(10): print(i)
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

    >>> type(ordersRDD)
    <class 'pyspark.rdd.RDD'>
    As we can see, ordersRDD is a RDD (distributed collection)


    >>> type(ordersRDD.first)
    <type 'instancemethod'>
    And the first element in the RDD is of type string 

```
So, to convert a string(each row) into a record having a structure, we can use Row against map.
To sum up, we will try to convert each element in the RDD to type Row and then, we will convert it to Dataframe
The difference between RDD and Dataframe is the structure (RDD does not have structure, whereas dataframe does have structure). The reason for using a dataframe is due to the fact that is easier to manage a dataframe comparing to RDD.
```python
    from pyspark.sql import Row
    ordersRDD = sc.textFile("/public/retail_db/orders")
    >>> ordersDF = ordersRDD.\
... map(lambda o: Row(order_id=int(o.split(",")[0]), order_date=o.split(",")[1], order_customer_id=int(o.split(",")[2]), order_status=o.split(",")[3])).toDF()

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
Earlier, when we used sqlContext to run queries, sqlContext was connected against.
In order to execute queries against that variable (ordersDF), we have to create a temp table.

```python
    ordersDF.registerTempTable("ordersDF_table")
    >>> sqlContext.sql("select order_status, count(1) from ordersDF_table group by order_status").show()
    +---------------+-----+                                                         
    |   order_status|  _c1|
    +---------------+-----+
    |        PENDING| 7610|
    |        ON_HOLD| 3798|
    | PAYMENT_REVIEW|  729|
    |PENDING_PAYMENT|15030|
    |     PROCESSING| 8275|
    |         CLOSED| 7556|
    |       COMPLETE|22899|
    |       CANCELED| 1428|
    |SUSPECTED_FRAUD| 1558|
    +---------------+-----+

    Now we have to get product data from the local file system
    productsRaw = open("/data/retail_db/products/part-00000").read().splitlines()
    >>> type(productsRaw)
    <type 'list'>

    as the result is of type list, we can convert it to RDD to print it:
    >>> productsRDD = sc.parallelize(productsRaw)
    >>> for i in productsRDD.take(10):print(i)
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

    productDf = productsRDD.\
    map(lambda p: Row(product_id=int(p.split(",")[0]), product_name = p.split(",")[2])).\
    toDF()
    >>> for ir in productDf.take(10): print(i)
    ... 
    10,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
    10,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
    10,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
    10,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
    10,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
    10,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
    10,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
    10,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
    10,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
    10,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat
    >>> 
    productsDf.registerTempTable("products")
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

```
Now, we have to use this data to join with order and order_items from Hive
To demonstrate that we have created properly the table, we show those that 
we have in the sqlContext:
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


    sqlContext.sql("select * from orders").show()
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

From order_items table we will need "order_item_product_id" to join with product table (but in the end we dor't need that column. In spite of susing it, we need the product_name column from product table) and order_item_subtotal to get the revenue. So, the steps are:

    1. we need to join order_item and product to get product name).


2. Join order_items with orders becase we need the order_date column from orders table. In addition, we need to filter the rows in COMPLETE or CLOSED state.
If we have aggregate functions with other column names, tipically we have to use group by, that's the reason why we have to use GROUP BY o.order_date, p.product_name (we want to have the total revenue per product)
(we use sum(oi.order_item_subtotal) because we need daily revenue per product)
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


If you want to reduce the number of tasks for this operation, we will use before executing the sql Query

```python
        sqlContext.setConf("spark.sql.shuffle.partitions", "2")
```


### WRITE SPARK APPLICATION - SAVING DATAFRAME TO HIVE TABLES

Now, we will use Hive to store the output. The name of the database will be carlos_sanchez_daily_revenue
```python
        >>> sqlContext.sql("CREATE DATABASE carlos_sanchez_daily_revenue");
        DataFrame[result: string]
```
- Get order_date, product_name, daily_revenue_per_product and save it into a HIVE table using ORC file format
```python    
        >>> sqlContext.sql("CREATE TABLE carlos_sanchez_daily_revenue.daily_revenue(order_date string, product_name string, daily_revenue_per_product float) stored as orc")
        DataFrame[result: string]
```
- Then, we will select the info we want to insert into our new table:
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
And now, we insert that info into the table:
```python    
        daily_revenue_per_product_df.write.insertInto("carlos_sanchez_daily_revenue.daily_revenue")
```
We check the info from the table we have created
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
        +--------------------+--------------------+------------
```
If we want help in order to know what are the functions available as for our dataframa, we will use:
```python
        help(daily_revenue_per_product_df)
```
### DATAFRAME OPERATIONS 
    Let us explore a few Dataframe operations:
        + show()
        + select
        + filter
        + join
        ...

To see the operations available in a dataframe, we can do:
```python
help(daily_revenue_per_product_df)
```
In case we want to have help related with a certain function, we will use help(daily_revenue_per_product_df.write)
To show the schema of a dataframe:
```python    
        >>> daily_revenue_per_product_df.schema
        StructType(List(StructField(order_date,StringType,true),StructField(product_name,StringType,true),StructField(daily_revenue_per_product,DoubleType,true)))


        >>> daily_revenue_per_product_df.printSchema()
        root
         |-- order_date: string (nullable = true)
         |-- product_name: string (nullable = true)
         |-- daily_revenue_per_product: double (nullable = true)
```
In order to save this data, we will have to:
```python    
    >>> daily_revenue_per_product_df.insertInto

    >>> daily_revenue_per_product_df.saveAsTable --> It will create the table an load the data info into it

    >>> daily_revenue_per_product_df.saveAsTable --> It will create the table an load the data info into it

    >>> daily_revenue_per_product_df.save("/user/carlos_sanchez/daily_revenue_save", "json")

            it's equivalent to:

    >>> daily_revenue_per_product_df.write.json("/user/carlos_sanchez/daily_revenue_save")
```

If we check whether the files have been saved:

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

If we want to filter some data:
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
or we can use:
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


### LAUNCHING SPARK 
we can do it in two ways:
```python
    1. export SPARK_MAJOR_VERSION=2 and then run spark-shell, pyspark, spark-sql ---> Horton

    2.  spark2-shell or pyspark2 or spark2-sql or spark2-submit ---> Cloudera (recommended)
```
### SPARK MODULES

RDD -> distributed collection
Dataframe -> distributed collection with structure (it is like a table)

```python
[carlos_sanchez@gw03 ~]$ hdfs dfs -ls /public/randomtextwriter

Found 31 items
-rw-r--r--   3 training hdfs          0 2017-01-18 20:24 /public/randomtextwriter/_SUCCESS
-rw-r--r--   3 training hdfs 1102230331 2017-01-18 20:24 /public/randomtextwriter/part-m-00000
-rw-r--r--   3 training hdfs 1102241707 2017-01-18 20:24 /public/randomtextwriter/part-m-00001
-rw-r--r--   3 training hdfs 1102239201 2017-01-18 20:24 /public/randomtextwriter/part-m-00002
-rw-r--r--   3 training hdfs 1102229785 2017-01-18 20:23 /public/randomtextwriter/part-m-00003
-rw-r--r--   3 training hdfs 1102239085 2017-01-18 20:24 /public/randomtextwriter/part-m-00004
-rw-r--r--   3 training hdfs 1102245185 2017-01-18 20:24 /public/randomtextwriter/part-m-00005
-rw-r--r--   3 training hdfs 1102242635 2017-01-18 20:24 /public/randomtextwriter/part-m-00006
-rw-r--r--   3 training hdfs 1102227833 2017-01-18 20:24 /public/randomtextwriter/part-m-00007
-rw-r--r--   3 training hdfs 1102244768 2017-01-18 20:24 /public/randomtextwriter/part-m-00008
-rw-r--r--   3 training hdfs 1102244831 2017-01-18 20:24 /public/randomtextwriter/part-m-00009
-rw-r--r--   3 training hdfs 1102233053 2017-01-18 20:24 /public/randomtextwriter/part-m-00010
-rw-r--r--   3 training hdfs 1102228278 2017-01-18 20:24 /public/randomtextwriter/part-m-00011
-rw-r--r--   3 training hdfs 1102239114 2017-01-18 20:24 /public/randomtextwriter/part-m-00012
-rw-r--r--   3 training hdfs 1102235849 2017-01-18 20:24 /public/randomtextwriter/part-m-00013
-rw-r--r--   3 training hdfs 1102238892 2017-01-18 20:24 /public/randomtextwriter/part-m-00014
-rw-r--r--   3 training hdfs 1102237321 2017-01-18 20:23 /public/randomtextwriter/part-m-00015
-rw-r--r--   3 training hdfs 1102236889 2017-01-18 20:24 /public/randomtextwriter/part-m-00016
-rw-r--r--   3 training hdfs 1102244070 2017-01-18 20:24 /public/randomtextwriter/part-m-00017
-rw-r--r--   3 training hdfs 1102228803 2017-01-18 20:23 /public/randomtextwriter/part-m-00018
-rw-r--r--   3 training hdfs 1102231608 2017-01-18 20:24 /public/randomtextwriter/part-m-00019
-rw-r--r--   3 training hdfs 1102228641 2017-01-18 20:24 /public/randomtextwriter/part-m-00020
-rw-r--r--   3 training hdfs 1102235104 2017-01-18 20:24 /public/randomtextwriter/part-m-00021
-rw-r--r--   3 training hdfs 1102227744 2017-01-18 20:24 /public/randomtextwriter/part-m-00022
-rw-r--r--   3 training hdfs 1102226515 2017-01-18 20:24 /public/randomtextwriter/part-m-00023
-rw-r--r--   3 training hdfs 1102224814 2017-01-18 20:24 /public/randomtextwriter/part-m-00024
-rw-r--r--   3 training hdfs 1102224114 2017-01-18 20:24 /public/randomtextwriter/part-m-00025
-rw-r--r--   3 training hdfs 1102242859 2017-01-18 20:24 /public/randomtextwriter/part-m-00026
-rw-r--r--   3 training hdfs 1102252598 2017-01-18 20:24 /public/randomtextwriter/part-m-00027
-rw-r--r--   3 training hdfs 1102244434 2017-01-18 20:24 /public/randomtextwriter/part-m-00028
-rw-r--r--   3 training hdfs 1102232634 2017-01-18 20:24 /public/randomtextwriter/part-m-00029

```
To see metadata about the file we execute the next command:
```python
[carlos_sanchez@gw03 ~]$ hadoop fsck /public/randomtextwriter/part-m-00000
DEPRECATED: Use of this script to execute hdfs command is deprecated.
Instead use the hdfs command for it.

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
The number of blocks are determined by the size of the file to be computed. As the file has more than 1 GB and each block size is, by default 128 MB, we will need 9 blocks to compute the file.
We can change the default number of nodes by using the "minPartition" attribute. Above we show how to make the file to be computed and divided into 18 portions.
```python
data = sc.textFile("/public/randomtextwriter/part-m-00000", minPartitions= 18)
```
If we tried to associate less partitions than 9, we won't be able to do it as the minimum number of partitions by default is 9.

```python
SparkContext available as sc, HiveContext available as sqlContext.
>>> sc
<pyspark.context.SparkContext object at 0x7f250a09fe90>
```
The RDD's will be created from the sc (read files...)
Now, we read the file containing random words and then, execute the word count.

With RDD:
```python
>>> data = sc.textFile("/public/randomtextwriter/part-00000")
>>> type(data)
<class 'pyspark.rdd.RDD'>
```
We will use as we want to execute a function to each and every line of the fil
and we want to separate each of the word in the line by the " " character (each line will be divided into an array  and, because we are using flatmap that array of words will be flatten into individual records)

```python
wc = data.flatMap(lambda line: line.split(" ")). \
     map(lambda word: (word, 1))                  -----------> 


wc = data.flatMap(lambda line: line.split(" ")). \
map(lambda word: (word, 1)). \
reduceByKey(lambda x,y: x+y)     

>>> for i in wc.take(10): print(i)
... 
[Stage 0:>                                                          (0 + 1) / 9]
(u'\x00\x00\x03', 3713)                                                         
(u'', 1)
(u'\ufffd\x02%wingable', 1)
(u'\x00\x00\x03;\x00\x00\x00FEblightbird', 1)
(u'\ufffd\ufffd\ufffd\ufffd\ufffdg\x05\x081\ufffd\ufffdJ$\ufffd\u05d2\x1ad\ufffd\x08\x00\x00\x045\x00\x00\x00DCcounterappellant', 1)
(u'\ufffd\ufffd\ufffd\ufffd\ufffdg\x05\x081\ufffd\ufffdJ$\ufffd\u05d2\x1ad\ufffd\x08\x00\x00\x02\ufffd\x00\x00\x00XWredesertion', 2)
(u'\ufffd\ufffd\ufffd\ufffd\ufffdg\x05\x081\ufffd\ufffdJ$\ufffd\u05d2\x1ad\ufffd\x08\x00\x00\x01\x17\x00\x00\x00?>glossing', 1)
(u'\x00\x00\x00\ufffd\x00\x00\x00`_unpredict', 1)
(u'\ufffd\ufffd\ufffd\ufffd\ufffdg\x05\x081\ufffd\ufffdJ$\ufffd\u05d2\x1ad\ufffd\x08\x00\x00\x01\ufffd\x00\x00\x00ONbreadwinner', 1)
(u'\ufffd\x02Uretinize', 3)
```
Now, if we want to save the content to a file without parenthesis signs, we will perform the next operation:

```python
>> wc.map(lambda rec: rec[0]+ ',' + str(rec[1])). \
... saveAsTextFile('/user/carlos_sanchez/core/wordcount')
```
And we check whether the files have been generated:
```python
[carlos_sanchez@gw03 ~]$ hdfs dfs -ls /user/carlos_sanchez/core/wordcount
Found 10 items
-rw-r--r--   2 carlos_sanchez hdfs          0 2020-07-20 11:51 /user/carlos_sanchez/core/wordcount/_SUCCESS
-rw-r--r--   2 carlos_sanchez hdfs    5314970 2020-07-20 11:51 /user/carlos_sanchez/core/wordcount/part-00000
-rw-r--r--   2 carlos_sanchez hdfs    5295033 2020-07-20 11:51 /user/carlos_sanchez/core/wordcount/part-00001
-rw-r--r--   2 carlos_sanchez hdfs    5300190 2020-07-20 11:51 /user/carlos_sanchez/core/wordcount/part-00002
-rw-r--r--   2 carlos_sanchez hdfs    5316876 2020-07-20 11:51 /user/carlos_sanchez/core/wordcount/part-00003
-rw-r--r--   2 carlos_sanchez hdfs    5338068 2020-07-20 11:51 /user/carlos_sanchez/core/wordcount/part-00004
-rw-r--r--   2 carlos_sanchez hdfs    5315111 2020-07-20 11:51 /user/carlos_sanchez/core/wordcount/part-00005
-rw-r--r--   2 carlos_sanchez hdfs    5297524 2020-07-20 11:51 /user/carlos_sanchez/core/wordcount/part-00006
-rw-r--r--   2 carlos_sanchez hdfs    5295690 2020-07-20 11:51 /user/carlos_sanchez/core/wordcount/part-00007
-rw-r--r--   2 carlos_sanchez hdfs    5319610 2020-07-20 11:51 /user/carlos_sanchez/core/wordcount/part-00008
```
And we can check the content:
```python
[carlos_sanchez@gw03 ~]$ hdfs dfs -tail /user/carlos_sanchez/core/wordcount/part-00000
WVskyshine,1
�����1��J$�גd�@?gallybeggar,2
�A@feasibleness,2
�87Swaziland,4
Q87outwealth,1
UTmerciful,1
aQPdevilwise,1
�����1��J$�גdQ_^sloped,1
f_^limpet,1
KJcresylite,1
:9farrantly,1
[carlos_sanchez@gw03 ~]$ hdfs dfs -tail /user/carlos_sanchez/core/wordcount/part-00000
WVskyshine,1
�����1��J$�גd�@?gallybeggar,2
�A@feasibleness,2
�87Swaziland,4
Q87outwealth,1
UTmerciful,1
aQPdevilwise,1
�����1��J$�גdQ_^sloped,1
f_^limpet,1
KJcresylite,1
:9farrantly,1
```


With Dataframes
----------------
(it is important that we have to launch pyspark2)
```python
data = sc.textFile('/public/randomtextwriter/part-m-00000')

#as pyspark.sql is not part of pyspark, we have to import it (split because we have to split each line into words, and then it will be flatten (for that reason we need the explode funtion))
from pyspark.sql.functions import split, explode

>>> wc = data.select(explode(split(data.value, ' ')).alias('words')). \  -----> we are naming the column as "words"
...   groupBy('words'). \
...   count()
>>> wc.show()

+--------------------+-----+                                                    
|               words|count|
+--------------------+-----+
|              corbel|96734|
|          lebensraum|97677|
|          Spencerism|97864|
|         ��theologal|  228|
|�YXsubirrigate|    3|
|�WVextraorg...|    3|
|         ��arsenide|  223|
|�����1��J$�גd...|    1|
|    �54uvanite|    4|
|           �Lchasmy|    6|
|�����1��J$�גd...|    1|
|   �87gemmeous|    3|
|    }paleornithology|    3|
|  �>=naprapath|    5|
|    �
       Oryzorictinae|    2|
|   �unsupercilious|    2|
|�<;floatabi...|    5|
|   �FEMormyrus|    1|
|       ��trailmaking|  193|
|      �vgallybeggar|    1|
+--------------------+-----+
only showing top 20 rows
```

And we write the content to a csv file
```python
wc.write.csv ('/user/carlos_sanchez/df/wordcount') ---> we don't need to specofy a delimiter as it does it automatically
```
And we check whether the files have been created:
```python
[carlos_sanchez@gw03 ~]$ hdfs dfs -ls /user/carlos_sanchez/df/wordcount
Found 201 items
-rw-r--r--   2 carlos_sanchez hdfs          0 2020-07-21 04:10 /user/carlos_sanchez/df/wordcount/_SUCCESS
-rw-r--r--   2 carlos_sanchez hdfs     226555 2020-07-21 04:10 /user/carlos_sanchez/df/wordcount/part-00000-4055236b-8814-4b59-b674-d5c712cdd4e0-c000.csv
-rw-r--r--   2 carlos_sanchez hdfs     224023 2020-07-21 04:10 /user/carlos_sanchez/df/wordcount/part-00001-4055236b-8814-4b59-b674-d5c712cdd4e0-c000.csv
-rw-r--r--   2 carlos_sanchez hdfs     220236 2020-07-21 04:10 /user/carlos_sanchez/df/wordcount/part-00002-4055236b-8814-4b59-b674-d5c712cdd4e0-c000.csv
-rw-r--r--   2 carlos_sanchez hdfs     226886 2020-07-21 04:10 /user/carlos_sanchez/df/wordcount/part-00003-4055236b-8814-4b59-b674-d5c712cdd4e0-c000.csv
-rw-r--r--   2 carlos_sanchez hdfs     225043 2020-07-21 04:10 /user/carlos_sanchez/df/wordcount/part-00004-4055236b-8814-4b59-b674-d5c712cdd4e0-c000.csv
-rw-r--r--   2 carlos_sanchez hdfs     221281 2020-07-21 04:10 /user/carlos_sanchez/df/wordcount/part-00005-4055236b-8814-4b59-b674-d5c712cdd4e0-c000.csv
-rw-r--r--   2 carlos_sanchez hdfs     223579 2020-07-21 04:10 /user/carlos_sanchez/df/wordcount/part-00006-4055236b-8814-4b59-b674-d5c712cdd4e0-c000.csv
-rw-r--r--   2 carlos_sanchez hdfs     229711 2020-07-21 04:10 /user/carlos_sanchez/df/wordcount/part-00007-4055236b-8814-4b59-b674-d5c712cdd4e0-c000.csv
-rw-r--r--   2 carlos_sanchez hdfs     219091 2020-07-21 04:10 /user/carlos_sanchez/df/wordcount/part-00008-4055236b-8814-4b59-b674-d5c712cdd4e0-c000.csv
-rw-r--r--   2 carlos_sanchez hdfs     223617 2020-07-21 04:10 /user/carlos_sanchez/df/wordcount/part-00009-4055236b-8814-4b59-b674-d5c712cdd4e0-c000.csv
-rw-r--r--   2 carlos_sanchez hdfs     227049 2020-07-21 04:10 /user/carlos_sanchez/df/wordcount/part-00010-4055236b-8814-4b59-b674-d5c712cdd4e0-c000.csv
```

### SPARK FRAMEWORK 
Execution modes:

    * Local: for development
    * Standalone: for development
    * Mesos: Production
    * Yarn: Production


Yarn stands for "yet another resource negociator". It is built using Master/Slave architecture (Master  is the resource manager and slave is the node manager). 
Tipically we have 1 or 2 resource managers and hundreds of node managers distributing data nodes.
Yarn does not process the data. It primarily takes care of management as well as resource management as well as scheduling the tasks  that are available as part of the cluster.
For each and every yarn application that is submitted there will be an application master and then to process the data we will have containers (as for spark, those containers are nothing but executors). If you are using map-reduce those tasks are just map and reduce taks.
We can plugin different distributed frameworks into YARN, such as Map Reduce, Spark, etc.
Spark creates executors to process the data and these executors will be managed by the Resource Manageer and per job Application Master.
To submit jobs we have the spark-submit:
    - If you want to launch spark in python mode: pyspark
    - If you want to launch spark in scala mode: spark-shell
The spark mode (pyspark or spark-shell) will take care of the spark context and using the spark context you can start developing your application. 
The cluster manager, when it comes to yarn, will be the resource manager who takes care of the resource management of the cluster and for each job there is something called the "application master ". That application master will take care of certain aspects of the jobs.
So, to sum up:
- Cluster manager in yarn = resource manager + application master (to keep track of all execution jobs)

If we wanted to see how an action has been managed:
```python
>>> wc = data.flatMap(lambda line: line.split(" ")). \
...      map(lambda word: (word, 1))
>>> for i in wc.toDebugString().split('\n'):print(i)
... 
(9) PythonRDD[23] at RDD at PythonRDD.scala:48 []
 |  /public/randomtextwriter/part-m-00000 MapPartitionsRDD[21] at textFile at NativeMethodAccessorImpl.java:0 []
 |  /public/randomtextwriter/part-m-00000 HadoopRDD[20] at textFile at NativeMethodAccessorImpl.java:0 []



>>> wc. \
...     map(lambda rec: rec[0] + ',' + str(rec[1])). \
...     saveAsTextFile('/user/carlos_sanchez/core/wordcount10')
[Stage 5:>                                                          (0 + 9) / 9] ----> it is suing 9 tasks as part of the stage 0. Depending on certaing features it will determine how many taks it will use in the next stages

0 + 9) / 9] ---> It is executing 0 tasks out of 9 tasks
-

0 + 9) / 9] ---> It is executing in 9 stages
    - 

```

When we launch pyspark, it will created nodes as part of the spark context. when it starts processing the data it will create the tasks in the worker nodes. Executor tasks are the ones that will process the data. We only specify two executors by default, and each executor is configured with one core and one GB (It means it can only perform one task at a time). From the line above, we will execute 9 tasks, but the real capacity is 2. When the two tasks have completed, the rest of the tasks star running.
An application ins park might be divided into jobs and the jobs might be divided into multiple stages. When shuffling a new stage is created (reduceByKey implies a new stage, always)

```python
>>> ordersDF = spark.read.csv('/public/retail_db/orders')
>>> type(ordersDF)
<class 'pyspark.sql.dataframe.DataFrame'>


>>> ordersDF.first()
Row(_c0=u'1', _c1=u'2013-07-25 00:00:00.0', _c2=u'11599', _c3=u'CLOSED')
>>> 
```
As we can see, rows do not have names to refer to them. For that reason we can name columns and rows (by default, we see that all columns are given "c0", "c1"...)

```python
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

A dataframe can be created from any dataset which can be used using names.
One can create a dataframe by using data from files, hive tables, relational tables over JDBC.
Common functions on Dataframes:
    + printSchema - to print column names and data types of the dataframe
    + show - to preview data (default 20 records)

This statement will show the complete information about the table.
```python            
            >>> ordersDF.show(10, False)
            +---+---------------------+-----+---------------+                               
            |_c0|_c1                  |_c2  |_c3            |
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
            only showing top 10 rows
```

describe - to understand the characteristics of data
```python
            >>> ordersDF.describe()
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

+ count - to get number of records
+ collect - to convert dataframe into an array (or python collection)
Once the dataframe is created, we can process data using 2 approaches:
+ Native dataframe APIs
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
Register as temp table and run queries using spark.sql
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
```python
    python3 install -m pyspark ---> in the console 
    python3

    import pyspark
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('Create a Spark Session Demo').master('local').getOrCreate()
```


### Create dataframes from text files

+ We can use spark.read.csv or spark.read.text to read text data.
+ spark.read.csv can be used for comma separated data. Default field names will be in the form of _c0, _c1 etc
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
```python             
             ordersDF = spark.read.csv('/public/retail_db/orders', sep=',', schema = 'order_id int, order_date string, order_customer_id int, order_status string')
             >>> ordersDF.printSchema()
            root
             |-- order_id: integer (nullable = true)
             |-- order_date: string (nullable = true)
             |-- order_customer_id: integer (nullable = true)
             |-- order_status: string (nullable = true)
```
             Another way to do this is (WITHOUT DETERMINING DATA TYPES)
```python                          
             >>> ordersDF = spark.read.csv('/public/retail_db/orders', sep=',').toDF('order_id', 'order_date', 'order_customer_id', 'order_status')
```
             Another way:
```python             
                 ordersDF = read.format('csv').option('sep', ',').schema('order_id int, order_date string, order_customer_id int, order_status string'). \
                 load('/public/retail_db/orders')
```

Another way (typecasting the values)    :
```python             
             ordersDF.select(orders.order_id.cast("int"))
```

Another way:
```python             
             ordersDF.select(orders.order_id.cast("int"), orders.order_date, orders.order_customer_id.cast(IntegerType()),orders.order_status )
```
Another way:
```python             
             orders.withColumn('order_id', orders.order_id.cast("int")).\
                                withColumn('order_customer_id', orders.order_customer_id.cast(IntegerType()))
```


    - spark.read.text can be used to read fixed length data where there is no delimiter. Default field name is value.
```python
            orders= spark.read.text('/public/retail_db/orders')
            >>> orders.printSchema()
            root
            |-- value: string (nullable = true)

            As we can see, it consider the data as a string type            
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

+ We can define attribute names using toDF function
+ In either of the case data will be represented as strings
+ We can covert data types by using cast function 
            – df.select(df.field.cast(IntegerType()))
+ We will see all other functions soon, but let us perform the task of reading the data into data frame and represent it in their original format.  

### Creating dataframes from hive tables

If Hive and Spark are integrated, we can create data frames from data in Hive tables or run Spark SQL queries against it.
    We launch pyspark in the latest version
    pyspark2 --master yarn --conf spark.ui.port=0
    hive ---> to execute hive
    - We can use spark.read.table to read data from Hive tables into Data Frame
```python 
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
```
    - We can prefix database name to table name while reading Hive tables into Data Frame
    - We can also run Hive queries directly using spark.sql


```python    
    spark.sql('select * from carlos_sanchez_retail_db_txt.orders')
```
    - Both spark.read.table and spark.sql returns Data Frame

### Creating dataframes from hive tables
- In Pycharm, we need to copy relevant jdbc jar file to SPARK_HOME/jars
- We can either use spark.read.format(‘jdbc’) with options or spark.read.jdbc with jdbc url, table name and other properties as dict to read data from remote relational databases.

     We need to make sure jdbc jar file is registered using --packages or --jars and --driver-class-path while launching pyspark
        ** Example**
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

                help(spark.read.jdbc) ---> to search for information about jdbc
```

We connect to the database in order to check the database we have to connect to in our spark session
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


                Now we can read the table:
                orders = spark.read. \
                  format('jdbc'). \
                  option('url', 'jdbc:mysql://ms.itversity.com'). \
                  option('dbtable', 'retail_db.orders'). \
                  option('user', 'retail_user'). \
                  option('password', 'itversity'). \
                  load()

                 Another war to read from a table:
                 orderItems = spark.read. \
                    jdbc("jdbc:mysql://ms.itversity.com", "retail_db.order_items",
                    properties={"user": "retail_user",
                    "password": "itversity",
                    "numPartitions": "4",
                    "partitionColumn": "order_item_order_id",
                    "lowerBound": "10000",
                    "upperBound": "20000"})

```
Traditionally, we use Sqoop to ingest data from relational database or jdbc into hdfs, and process the data further using Hive or spark.sql We can move some of the sqoop applications into spark using additional parameters


-   Spark facilitates the way to import data from other origins: While reading data, we can define number of partitions (using numPartitions), criteria to divide data     into partitions (partitionColumn, lowerBound, upperBound)
    + To define the number of threads we can use the numPartitions
    + To define the number of columns we can use the partitionColumn (Partitioning can be done only on numeric fields)
    + To define boundary partition we have lowerbound and upperbound

### REVIEW 

If lowerBound and upperBound is specified, it will generate strides  depending up on number of partitions and then process entire data. Here is the example:
    + We are trying to read order_items data with 4 as numPartitions
    + partitionColumn – order_item_order_id
    + lowerBound – 10000
    + upperBound – 20000
    + order_item_order_id is in the range of 1 and 68883
    + But as we define lowerBound as 10000 and upperBound as 20000, here will be strides – 1 to 12499, 12500 to 14999, 15000 to 17499, 17500 to maximum of order_item_order_id
    + You can check the data in the output path mentioned


### DATAFRAME OPERATIONS

Let us see overview about Data Frame Operations. It is one of the 2 ways we can process Data Frames.

        + Selection or Projection – select
        + Filtering data – filter or where
        + Joins – join (supports outer join as well)
        + Aggregations – groupBy and agg with support of functions such as sum, avg, min, max etc
        + Sorting – sort or orderBy
        + Analytics Functions – aggregations, ranking and windowing functions

### SPARK SQL
```python
    from pyspark.sql import functions
    help(functions) ---> It will display the list of the functions that are available
```
We can also use Spark SQL to process data in data frames.

        + We can get list of tables by using spark.sql('show tables')
        + We can register data frame as temporary view df.createTempView("view_name")
        + Output of show tables show the temporary tables as well
        + Once temp view is created, we can use SQL style syntax and run queries against the tables/views
        + Most of the hive queries will work out of the box
        + Main package for functions pyspark.sql.functions
        + We can import by saying from pyspark.sql import functions as sf
        + You will see many functions which are similar to the functions in traditional databases.
        + These can be categorized into
            - String manipulation
            - Date manipulation
            - Type casting
            - Expressions such as case when
        + We will see some of the functions in action
            - substring
            - lower, upper
            - transient_lastDdlTime
            - date_format
            - trunc
            - Type Casting
            - case when

If we want to have help on some of the functions we will do
```python
 help(functions.subtring)
```
 or
```python
spark.sql('describe function substring').show(truncate=False)
```


### PROCESSING DATA USING DATA FRAMES 
**Problem**
Get daily revenue per product. 

Ideas
------
+ If we want to group data by product, what we will have to do is to use any table having a product_id column. (for example, the products table)
+ If we want the product_name we have to go to the product table as well
+ To keep it simple, we get daily revenue on product_id
+ product_id is available in order_items table (order_item_product_id attribute)
+ the date is in orders table as both have different information that we have to share, we have to join both tables to get the information for daily product.
  However, we want to get daily revenue per each product per each day
+ Data is comma separated
+ We will fetch data using spark.read.csv
+ Apply type cast functions to convert fields into their original type where ever is applicable.


Fields of the tables to be taken into account:

  Orders
  ------
  order_id, order_date, order_customer_id, order_status


  Order_items
  -----------
  order_item_id, order_item_order_id, order_item_product_id, order_item_quantity, order_item_subtotal, order_item_product_price


  The join keys are order_id(orders table) and order_item_order_id (order_items).


  Solution:
  -----------
  python3

  (once inside the python session we create the spark session programatically):

```python

  >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession. \
    ...   builder. \
    ...   master('local'). \
    ...   appName('CSV Example'). \
    ...   getOrCreate()
```
Now, we create the orders dataframe defining the data type

```python

    orders = spark.read. \   
      format('csv'). \
      schema('order_id int, order_date string, order_customer_id int, order_status string'). \
      load('/public/retail_db/orders')

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


And we do the same with order_items table:
```python

    orderItemsCSV = spark.read.csv('/public/retail_db/order_items'). \
        toDF('order_item_id', 'order_item_order_id', 'order_item_product_id','order_item_quantity', 'order_item_subtotal', 'order_item_product_price')

    >>> orderItemsCSV.show()
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
As we can see, there are numeric fields that we have to cast to its numerical type
```python

    >>> orderItemsCSV.printSchema()
    root
     |-- order_item_id: string (nullable = true)
     |-- order_item_order_id: string (nullable = true)
     |-- order_item_product_id: string (nullable = true)
     |-- order_item_quantity: string (nullable = true)
     |-- order_item_subtotal: string (nullable = true)
     |-- order_item_product_price: string (nullable = true)
```
So we cast order and order_items table with it correct data type:
```python

     >>> orders = ordersCSV. \
    ...   withColumn('order_id', ordersCSV.order_id.cast(IntegerType())). \
    ...   withColumn('order_customer_id', ordersCSV.order_customer_id.cast(IntegerType()))
    >>> orders.printSchema()
    root
     |-- order_id: integer (nullable = true)
     |-- order_date: string (nullable = true)
     |-- order_customer_id: integer (nullable = true)
     |-- order_status: string (nullable = true)


from pyspark.sql.types import IntegerType, FloatType

     orderItems = orderItemsCSV.\
    withColumn('order_item_id', orderItemsCSV.order_item_id.cast(IntegerType())). \
    withColumn('order_item_order_id', orderItemsCSV.order_item_order_id.cast(IntegerType())). \
    withColumn('order_item_product_id', orderItemsCSV.order_item_product_id.cast(IntegerType())). \
    withColumn('order_item_quantity', orderItemsCSV.order_item_quantity.cast(IntegerType())). \
    withColumn('order_item_subtotal', orderItemsCSV.order_item_subtotal.cast(FloatType())). \
    withColumn('order_item_product_price', orderItemsCSV.order_item_product_price.cast(FloatType()))

    >>> orderItems.printSchema()
    root
     |-- order_item_id: integer (nullable = true)
     |-- order_item_order_id: integer (nullable = true)
     |-- order_item_product_id: integer (nullable = true)
     |-- order_item_quantity: integer (nullable = true)
     |-- order_item_subtotal: float (nullable = true)
     |-- order_item_product_price: float (nullable = true)
```
Now we can see that we have the correct data types for each field in the tables.
In newer versions of spark we can load the data this way, defining the data type of the fields while loading the data:
```python

     orders = spark.read. \
      format('csv'). \
      schema('order_id int, order_date string, order_customer_id int, order_status string'). \
      load('/public/retail_db/orders')
```
However, if we are using older versions this cannot be done. For that reason we have to load and define the data types of the fields in two steps:

1. Read csv file and convert it to a dataframe:
```python     
     orderItemsCSV = spark.read. \
                csv('/public/retail_db/order_items'). \
                toDF('order_item_id', 'order_item_order_id', 'order_item_product_id', 
                    'order_item_quantity', 'order_item_subtotal', 'order_item_product_price')
```

2. Define data types: 
```python

            from pyspark.sql.types import IntegerType, FloatType

                orders = ordersCSV. \
                  withColumn('order_id', ordersCSV.order_id.cast(IntegerType())). \
                  withColumn('order_customer_id', ordersCSV.order_customer_id.cast(IntegerType()))

```

Projecting or selecting data from dataframes

  +  we can use select and fetch data from the fields we are lookin for.
  + Both orders and orderItems are of type dataFrames. We will be able to access attributes by prefixing dataframe names (orders.order_id and orderItems.order_item_id). Also, we can pass attributes names as strings:
Example:
```python
orders.select(ordes.order_id, orders.order_date)
```
it's equivalent to
```python
orders.select('order_id', 'order_date')
```
We can apply necessary functions to manipulate data while it is being projected
Example:

```python
orders.select(substring('order_date',1,7)).show()
```
We can give aliases to the derived fields using alias function 
Example: 
```python
orders.select(substring('order_date',1,8).alias('order_month')).show()
```
We can do it in sql way:

```python
>>> orders.selectExpr('substring(order_date,1,7) as order_month')
DataFrame[order_month: string]
```
To preview the data, we can do:
```python

        >>> orders.selectExpr('substring(order_date,1,7) as order_month').show()
        +-----------+                                                                   
        |order_month|
        +-----------+
        |    2013-07|
        |    2013-07|
        |    2013-07|
        |    2013-07|
        |    2013-07|
        |    2013-07|
        |    2013-07|
        |    2013-07|
        |    2013-07|
        |    2013-07|
        |    2013-07|
        |    2013-07|
        |    2013-07|
        |    2013-07|
        |    2013-07|
        |    2013-07|
        |    2013-07|
        |    2013-07|
        |    2013-07|
        |    2013-07|
        +-----------+
        only showing top 20 rows

```
### Filtering data
   

You can filter in one of the next ways (using filter or where, both are the same)
```python  
    help(orders.where)
```
   1.  takes dataframes native style syntax
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
2.  SQL syntax
```python

            >>> orders.where(orders.order_status == 'COMPLETE').show()
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

Now, we have some other requirements:
Get orders which are COMPLETE or CLOSED (we are going to do it in spark-way)
```python

                    >>> orders.filter((orders.order_status== 'COMPLETE') | (orders.order_status =='CLOSED')).show()
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

                        in sql way

                    >>> orders.filter("order_status = 'COMPLETE' or order_status ='CLOSED'").show()
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
We could have executed the query with the "in" statement:
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

In SQL way syntax:
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
2.  Get orders which are either COMPLETE or CLOSED and placed in the month of 2013 August                
```python

                    >>> orders.filter((orders.order_status.isin('COMPLETE', 'CLOSED')) &
                    ...                               (orders.order_date.like('2013-08%'))).show()
                    +--------+--------------------+-----------------+------------+                  
                    |order_id|          order_date|order_customer_id|order_status|
                    +--------+--------------------+-----------------+------------+
                    |    1297|2013-08-01 00:00:...|            11607|    COMPLETE|
                    |    1298|2013-08-01 00:00:...|             5105|      CLOSED|
                    |    1299|2013-08-01 00:00:...|             7802|    COMPLETE|
                    |    1302|2013-08-01 00:00:...|             1695|    COMPLETE|
                    |    1304|2013-08-01 00:00:...|             2059|    COMPLETE|
                    |    1305|2013-08-01 00:00:...|             3844|    COMPLETE|
                    |    1307|2013-08-01 00:00:...|             4474|    COMPLETE|
                    |    1309|2013-08-01 00:00:...|             2367|      CLOSED|
                    |    1312|2013-08-01 00:00:...|            12291|    COMPLETE|
                    |    1314|2013-08-01 00:00:...|            10993|    COMPLETE|
                    |    1315|2013-08-01 00:00:...|             5660|    COMPLETE|
                    |    1318|2013-08-01 00:00:...|             4212|    COMPLETE|
                    |    1319|2013-08-01 00:00:...|             3966|    COMPLETE|
                    |    1320|2013-08-01 00:00:...|            12270|    COMPLETE|
                    |    1321|2013-08-01 00:00:...|              800|    COMPLETE|
                    |    1322|2013-08-01 00:00:...|             9264|    COMPLETE|
                    |    1323|2013-08-01 00:00:...|             7422|    COMPLETE|
                    |    1324|2013-08-01 00:00:...|             4600|    COMPLETE|
                    |    1327|2013-08-01 00:00:...|             9526|    COMPLETE|
                    |    1329|2013-08-01 00:00:...|             6070|      CLOSED|
                    +--------+--------------------+-----------------+------------+
                    only showing top 20 rows
```
We can use the __and__ and __or__ operators  in spite of the "&" operator
We can do it in SQL style as well:
```python

                    >>> orders.filter("order_status in ('COMPLETE', 'CLOSED') and order_date like '2013-08%'").show()
                    +--------+--------------------+-----------------+------------+                  
                    |order_id|          order_date|order_customer_id|order_status|
                    +--------+--------------------+-----------------+------------+
                    |    1297|2013-08-01 00:00:...|            11607|    COMPLETE|
                    |    1298|2013-08-01 00:00:...|             5105|      CLOSED|
                    |    1299|2013-08-01 00:00:...|             7802|    COMPLETE|
                    |    1302|2013-08-01 00:00:...|             1695|    COMPLETE|
                    |    1304|2013-08-01 00:00:...|             2059|    COMPLETE|
                    |    1305|2013-08-01 00:00:...|             3844|    COMPLETE|
                    |    1307|2013-08-01 00:00:...|             4474|    COMPLETE|
                    |    1309|2013-08-01 00:00:...|             2367|      CLOSED|
                    |    1312|2013-08-01 00:00:...|            12291|    COMPLETE|
                    |    1314|2013-08-01 00:00:...|            10993|    COMPLETE|
                    |    1315|2013-08-01 00:00:...|             5660|    COMPLETE|
                    |    1318|2013-08-01 00:00:...|             4212|    COMPLETE|
                    |    1319|2013-08-01 00:00:...|             3966|    COMPLETE|
                    |    1320|2013-08-01 00:00:...|            12270|    COMPLETE|
                    |    1321|2013-08-01 00:00:...|              800|    COMPLETE|
                    |    1322|2013-08-01 00:00:...|             9264|    COMPLETE|
                    |    1323|2013-08-01 00:00:...|             7422|    COMPLETE|
                    |    1324|2013-08-01 00:00:...|             4600|    COMPLETE|
                    |    1327|2013-08-01 00:00:...|             9526|    COMPLETE|
                    |    1329|2013-08-01 00:00:...|             6070|      CLOSED|
                    +--------+--------------------+-----------------+------------+
                    only showing top 20 rows
```
3. Get order_items where order_item_subtotal is not equal to the product of order_item_quantity and order_item_product_price

When we select the next columns:
```python

            >>> orderItems.select('order_item_subtotal', 'order_item_quantity', 'order_item_product_price').show()
            [Stage 14:>                                                         (0 + 0) / 1]
            +-------------------+-------------------+------------------------+              
            |order_item_subtotal|order_item_quantity|order_item_product_price|
            +-------------------+-------------------+------------------------+
            |             299.98|                  1|                  299.98|
            |             199.99|                  1|                  199.99|
            |              250.0|                  5|                    50.0|
            |             129.99|                  1|                  129.99|
            |              49.98|                  2|                   24.99|
            |             299.95|                  5|                   59.99|
            |              150.0|                  3|                    50.0|
            |             199.92|                  4|                   49.98|
            |             299.98|                  1|                  299.98|
            |             299.95|                  5|                   59.99|
            |              99.96|                  2|                   49.98|
            |             299.98|                  1|                  299.98|
            |             129.99|                  1|                  129.99|
            |             199.99|                  1|                  199.99|
            |             299.98|                  1|                  299.98|
            |              79.95|                  5|                   15.99|
            |             179.97|                  3|                   59.99|
            |             299.95|                  5|                   59.99|
            |             199.92|                  4|                   49.98|
            |               50.0|                  1|                    50.0|
            +-------------------+-----

```
We want to ensure that order_item_product_price is calculated correctly. For thar purpose we will check, for each row, that order_item_subtotal = order_item_quantity * order_item_product_price
```python

            >>> orderItems. \
                ...             select('order_item_subtotal', 'order_item_quantity', 'order_item_product_price'). \
                ...             where ( orderItems.order_item_subtotal != orderItems.order_item_quantity * orderItems.order_item_product_price).show()
                +-------------------+-------------------+------------------------+              
                |order_item_subtotal|order_item_quantity|order_item_product_price|
                +-------------------+-------------------+------------------------+
                |             499.95|                  5|                   99.99|
                |             499.95|                  5|                   99.99|
                |             199.95|                  5|                   39.99|
                |             499.95|                  5|                   99.99|
                |             199.95|                  5|                   39.99|
                |             499.95|                  5|                   99.99|
                |             199.95|                  5|                   39.99|
                |             199.95|                  5|                   39.99|
                |             499.95|                  5|                   99.99|
                |             199.95|                  5|                   39.99|
                |             199.95|                  5|                   39.99|
                |             499.95|                  5|                   99.99|
                |             499.95|                  5|                   99.99|
                |             199.95|                  5|                   39.99|
                |             199.95|                  5|                   39.99|
                |             499.95|                  5|                   99.99|
                |             199.95|                  5|                   39.99|
                |             239.95|                  5|                   47.99|
                |             199.95|                  5|                   39.99|
                |             499.95|                  5|                   99.99|
                +-------------------+-------------------+------------------------+
                only showing top 20 rows

```
As we can see there are some incorrect values. As we can see, we have to round the first column because if not doing so, when we multiply there will be certaing rows shown as not well calculated despite the fact that the error comes due to decimals Now, we will fix it:

```python

                >>> from pyspark.sql.functions import round
                >>> orderItems. \
                ...                             select('order_item_subtotal', 'order_item_quantity', 'order_item_product_price'). \
                ...                             where(orderItems.order_item_subtotal!= round(orderItems.order_item_quantity *  orderItems.order_item_product_price)).show()     
                +-------------------+-------------------+------------------------+              
                |order_item_subtotal|order_item_quantity|order_item_product_price|
                +-------------------+-------------------+------------------------+
                |             299.98|                  1|                  299.98|
                |             199.99|                  1|                  199.99|
                |             129.99|                  1|                  129.99|
                |              49.98|                  2|                   24.99|
                |             299.95|                  5|                   59.99|
                |             199.92|                  4|                   49.98|
                |             299.98|                  1|                  299.98|
                |             299.95|                  5|                   59.99|
                |              99.96|                  2|                   49.98|
                |             299.98|                  1|                  299.98|
                |             129.99|                  1|                  129.99|
                |             199.99|                  1|                  199.99|
                |             299.98|                  1|                  299.98|
                |              79.95|                  5|                   15.99|
                |             179.97|                  3|                   59.99|
                |             299.95|                  5|                   59.99|
                |             199.92|                  4|                   49.98|
                |             199.98|                  2|                   99.99|
                |             199.99|                  1|                  199.99|
                |             199.99|                  1|                  199.99|
                +-------------------+-------------------+------------------------+
                only showing top 20 rows

```

Again, the query is showing as incorrect rows well calculated, so we have to redo the query again to show only the real mistaken ones.

```python

                select('order_item_subtotal', 'order_item_quantity', 'order_item_product_price'). \
                    where(orderItems.order_item_subtotal!= round(orderItems.order_item_quantity *  orderItems.order_item_product_price,2)).show() 
                +-------------------+-------------------+------------------------+
                |order_item_subtotal|order_item_quantity|order_item_product_price|
                +-------------------+-------------------+------------------------+
                +-------------------+-------------------+------------------------+
                           

                As we can see, there is not any incorrect row.
```
4. Get all the orders which are placed on first of every month
We can get the min date with the min function:

```python

            >>> from pyspark.sql.functions import min
            >>> orders.select(min(orders.order_date)).show()
            +--------------------+                                                          
            |     min(order_date)|
            +--------------------+
            |2013-07-25 00:00:...|
            +--------------------+



            With this, we can get the year of a date:

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

            To get the month we dan do:
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
            +---------------------------+
            only showing top 20 rows
```

To get the dady of month we dan do:

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
To get the day of month we dan do:
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


We can do it with sql stype syntax:
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
### JOINING DATASETS
   
Quite often we need to deal with multiple datasets which are related to each other.

  + We need to first understand the relationship with respect to datasets
  + All our datasets have relationships defined between them
    + order and order_items are transaction tables. order is parent and order_items is child. The relationship is established between the two using order_id (in order_items, it is represented as order_item_order_id)
  + We also have product catalogue normalized into 3 tables - products, categories and departments (with relationships established in that order)
  + We also have a customer table
  + There is a relationship between customers and orders - customers is parent dataset as one customer can place multiple orders.
  + There is a relationship between the product catalog and order_items via products - products is parent dataset as one product cam be ordered as part of multiple order_items
  + Determine the type of join - inner or outer (left or right or full)
  + Dataframes have an API called join to perform joins
  + We can make the outer join by passing an additional argument
   
Examples:

**Example 1**
        
Get all the order items corresponding to COMPLETE OR CLOSED orders
```python

        orders = spark.read. \
        ... format('csv'). \
        ... schema('order_id int, order_date string, order_customer_id int, order_status string'). \
        ... load('/public/retail_db/orders')


        orderItemsCSV = spark.read.csv('/public/retail_db/order_items'). \
        ... toDF('order_item_id', 'order_item_order_id', 'order_item_product_id','order_item_quantity', 'order_item_subtotal', 'order_item_product_price')

        from pyspark.sql.types import IntegerType, FloatType

        orderItems = orderItemsCSV.\
        ... withColumn('order_item_id', orderItemsCSV.order_item_id.cast(IntegerType())). \
        ...     withColumn('order_item_order_id', orderItemsCSV.order_item_order_id.cast(IntegerType())). \
        ...     withColumn('order_item_product_id', orderItemsCSV.order_item_product_id.cast(IntegerType())). \
        ...     withColumn('order_item_quantity', orderItemsCSV.order_item_quantity.cast(IntegerType())). \
        ...     withColumn('order_item_subtotal', orderItemsCSV.order_item_subtotal.cast(FloatType())). \
        ...     withColumn('order_item_product_price', orderItemsCSV.order_item_product_price.cast(FloatType()))



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
1.  What we want is to display filter the COMPLETE or CLOSED orders

            There are two possible solutions:
                - Filter the COMPLETE or CLOSED orders and then join with order_items (this is more efficient as susequent steps operate with less data)
                - Join with order_items and then filter the COMPLETE or CLOSED orders

            Let's get the first option (filterig first ):
```python
            Dataframe style
            ordersFiltered = orders.where(orders.order_status.isin('COMPLETE', 'CLOSED'))
```

SQL style:
```python

            ordersFiltered = orders.where("order_status in ('COMPLETE', 'CLOSED')")

```
Now, we execute the join in "dataframe style" ("inner" can be ommited because it will execute a inner join by default) :
```python

            >>> ordersJoin = ordersFiltered.join(orderItems, ordersFiltered.order_id == orderItems.order_item_order_id, 'inner')
            >>> type(ordersJoin)
            <class 'pyspark.sql.dataframe.DataFrame'>
```
2. Get all the orders where there are no corresponding order_items  

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
                     )  

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
            |order_id|          order_date|order_customer_id|   order_status|order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
            +--------+--------------------+-----------------+---------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
            |       1|2013-07-25 00:00:...|            11599|         CLOSED|            1|                  1|                  957|                  1|             299.98|                  299.98|
            |       2|2013-07-25 00:00:...|              256|PENDING_PAYMENT|            4|                  2|                  403|                  1|             129.99|                  129.99|
            |       2|2013-07-25 00:00:...|              256|PENDING_PAYMENT|            3|                  2|                  502|                  5|              250.0|                    50.0|
            |       2|2013-07-25 00:00:...|              256|PENDING_PAYMENT|            2|                  2|                 1073|                  1|             199.99|                  199.99|
            |       3|2013-07-25 00:00:...|            12111|       COMPLETE|         null|               null|                 null|               null|               null|                    null|
            |       4|2013-07-25 00:00:...|             8827|         CLOSED|            8|                  4|                 1014|                  4|             199.92|                   49.98|
            |       4|2013-07-25 00:00:...|             8827|         CLOSED|            7|                  4|                  502|                  3|              150.0|                    50.0|
            |       4|2013-07-25 00:00:...|             8827|         CLOSED|            6|                  4|                  365|                  5|             299.95|                   59.99|
            |       4|2013-07-25 00:00:...|             8827|         CLOSED|            5|                  4|                  897|                  2|              49.98|                   24.99|
            |       5|2013-07-25 00:00:...|            11318|       COMPLETE|           13|                  5|                  403|                  1|             129.99|                  129.99|
            |       5|2013-07-25 00:00:...|            11318|       COMPLETE|           12|                  5|                  957|                  1|             299.98|                  299.98|
            |       5|2013-07-25 00:00:...|            11318|       COMPLETE|           11|                  5|                 1014|                  2|              99.96|                   49.98|
            |       5|2013-07-25 00:00:...|            11318|       COMPLETE|           10|                  5|                  365|                  5|             299.95|                   59.99|
            |       5|2013-07-25 00:00:...|            11318|       COMPLETE|            9|                  5|                  957|                  1|             299.98|                  299.98|
            |       6|2013-07-25 00:00:...|             7130|       COMPLETE|         null|               null|                 null|               null|               null|                    null|
            |       7|2013-07-25 00:00:...|             4530|       COMPLETE|           16|                  7|                  926|                  5|              79.95|                   15.99|
            |       7|2013-07-25 00:00:...|             4530|       COMPLETE|           15|                  7|                  957|                  1|             299.98|                  299.98|
            |       7|2013-07-25 00:00:...|             4530|       COMPLETE|           14|                  7|                 1073|                  1|             199.99|                  199.99|
            |       8|2013-07-25 00:00:...|             2911|     PROCESSING|           20|                  8|                  502|                  1|               50.0|                    50.0|
            |       8|2013-07-25 00:00:...|             2911|     PROCESSING|           19|                  8|                 1014|                  4|             199.92|                   49.98|
            +--------+--------------------+-----------------+---------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
            only showing top 20 rows

```

As we can see, null values correspond to those orders that don't have anu order_item associated with them. So, to filter them what we have to do is no  filter null values:

```python

            >>> ordersLeftOuterJoin.where('order_item_order_id is null').show()
            +--------+--------------------+-----------------+---------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
            |order_id|          order_date|order_customer_id|   order_status|order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
            +--------+--------------------+-----------------+---------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
            |       3|2013-07-25 00:00:...|            12111|       COMPLETE|         null|               null|                 null|               null|               null|                    null|
            |       6|2013-07-25 00:00:...|             7130|       COMPLETE|         null|               null|                 null|               null|               null|                    null|
            |      22|2013-07-25 00:00:...|              333|       COMPLETE|         null|               null|                 null|               null|               null|                    null|
            |      26|2013-07-25 00:00:...|             7562|       COMPLETE|         null|               null|                 null|               null|               null|                    null|
            |      32|2013-07-25 00:00:...|             3960|       COMPLETE|         null|               null|                 null|               null|               null|                    null|
            |      40|2013-07-25 00:00:...|            12092|PENDING_PAYMENT|         null|               null|                 null|               null|               null|                    null|
            |      47|2013-07-25 00:00:...|             8487|PENDING_PAYMENT|         null|               null|                 null|               null|               null|                    null|
            |      53|2013-07-25 00:00:...|             4701|     PROCESSING|         null|               null|                 null|               null|               null|                    null|
            |      54|2013-07-25 00:00:...|            10628|PENDING_PAYMENT|         null|               null|                 null|               null|               null|                    null|
            |      55|2013-07-25 00:00:...|             2052|        PENDING|         null|               null|                 null|               null|               null|                    null|
            |      60|2013-07-25 00:00:...|             8365|PENDING_PAYMENT|         null|               null|                 null|               null|               null|                    null|
            |      76|2013-07-25 00:00:...|             6898|       COMPLETE|         null|               null|                 null|               null|               null|                    null|
            |      78|2013-07-25 00:00:...|             8619| PAYMENT_REVIEW|         null|               null|                 null|               null|               null|                    null|
            |      79|2013-07-25 00:00:...|             7327|PENDING_PAYMENT|         null|               null|                 null|               null|               null|                    null|
            |      80|2013-07-25 00:00:...|             3007|       COMPLETE|         null|               null|                 null|               null|               null|                    null|
            |      82|2013-07-25 00:00:...|             3566|PENDING_PAYMENT|         null|               null|                 null|               null|               null|                    null|
            |      85|2013-07-25 00:00:...|             1485|        PENDING|         null|               null|                 null|               null|               null|                    null|
            |      86|2013-07-25 00:00:...|             6680|PENDING_PAYMENT|         null|               null|                 null|               null|               null|                    null|
            |      89|2013-07-25 00:00:...|              824|        ON_HOLD|         null|               null|                 null|               null|               null|                    null|
            |      90|2013-07-25 00:00:...|             9131|         CLOSED|         null|               null|                 null|               null|               null|                    null|
            +--------+--------------------+-----------------+---------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
            only showing top 20 rows
```

3. Check if there are any order_items where there ir no corresponding order in the orders dataset.
For that purpose we have to perform a right outer join if orders is on the right. In addition, we can perform a left outer join in case order is on the
left side of the join.

```python

            orderItemsRightOuterJoin = orders. \
                join(orderItems,
                     orders.order_id == orderItems.order_item_order_id,
                     'right'---------> we could have used 'outer' as well)

            +--------+--------------------+-----------------+---------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
            |order_id|          order_date|order_customer_id|   order_status|order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
            +--------+--------------------+-----------------+---------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
            |       1|2013-07-25 00:00:...|            11599|         CLOSED|            1|                  1|                  957|                  1|             299.98|                  299.98|
            |       2|2013-07-25 00:00:...|              256|PENDING_PAYMENT|            2|                  2|                 1073|                  1|             199.99|                  199.99|
            |       2|2013-07-25 00:00:...|              256|PENDING_PAYMENT|            3|                  2|                  502|                  5|              250.0|                    50.0|
            |       2|2013-07-25 00:00:...|              256|PENDING_PAYMENT|            4|                  2|                  403|                  1|             129.99|                  129.99|
            |       4|2013-07-25 00:00:...|             8827|         CLOSED|            5|                  4|                  897|                  2|              49.98|                   24.99|
            |       4|2013-07-25 00:00:...|             8827|         CLOSED|            6|                  4|                  365|                  5|             299.95|                   59.99|
            |       4|2013-07-25 00:00:...|             8827|         CLOSED|            7|                  4|                  502|                  3|              150.0|                    50.0|
            |       4|2013-07-25 00:00:...|             8827|         CLOSED|            8|                  4|                 1014|                  4|             199.92|                   49.98|
            |       5|2013-07-25 00:00:...|            11318|       COMPLETE|            9|                  5|                  957|                  1|             299.98|                  299.98|
            |       5|2013-07-25 00:00:...|            11318|       COMPLETE|           10|                  5|                  365|                  5|             299.95|                   59.99|
            |       5|2013-07-25 00:00:...|            11318|       COMPLETE|           11|                  5|                 1014|                  2|              99.96|                   49.98|
            |       5|2013-07-25 00:00:...|            11318|       COMPLETE|           12|                  5|                  957|                  1|             299.98|                  299.98|
            |       5|2013-07-25 00:00:...|            11318|       COMPLETE|           13|                  5|                  403|                  1|             129.99|                  129.99|
            |       7|2013-07-25 00:00:...|             4530|       COMPLETE|           14|                  7|                 1073|                  1|             199.99|                  199.99|
            |       7|2013-07-25 00:00:...|             4530|       COMPLETE|           15|                  7|                  957|                  1|             299.98|                  299.98|
            |       7|2013-07-25 00:00:...|             4530|       COMPLETE|           16|                  7|                  926|                  5|              79.95|                   15.99|
            |       8|2013-07-25 00:00:...|             2911|     PROCESSING|           17|                  8|                  365|                  3|             179.97|                   59.99|
            |       8|2013-07-25 00:00:...|             2911|     PROCESSING|           18|                  8|                  365|                  5|             299.95|                   59.99|
            |       8|2013-07-25 00:00:...|             2911|     PROCESSING|           19|                  8|                 1014|                  4|             199.92|                   49.98|
            |       8|2013-07-25 00:00:...|             2911|     PROCESSING|           20|                  8|                  502|                  1|               50.0|                    50.0|
            +--------+--------------------+-----------------+---------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
            only showing top 20 rows

```

To assure that we have well implemented both joins, we can show the difference between both joins. If it is imoplemented correctly, there will not be any   row differentiating between both datasets.

```python


            >>> orderItemsRightOuterJoin.filter('order_id is null').show()
            +--------+----------+-----------------+------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
            |order_id|order_date|order_customer_id|order_status|order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
            +--------+----------+-----------------+------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
            +--------+----------+-----------------+------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
```


As we can see, there is not any row. That means we have well calcullated the rows without correspondence in the other table.
FullOuterJoin can only be perfomed in tables having a many to many relationship.


### GROUPING DATA AND PERFORMING AGGREGATIONS  
Many times we want to perform  aggregations such as sum, mimimum, maximum, etc within each group. We need to first group the data and them perform the aggregation.
        + groupBy is the function which can be used to group the data on one or more columns
        + once data is grouped we can perform all supported aggregations - sum, avg, min, max, etc.
        + we can invoke the functions directly as part of the agg
        + agg gives us more flexibility to give aliases to the derived fields.
        Examples:


To see all the functions available, we disconnect from the pyspark2 console (CTRL + D) and then we execute 'spark-shell'
```python

            scala> org.apache.spark.sql.functions. (and we enter TAB button). This functions will be shown:
            scala> org.apache.spark.sql.functions.
            abs                           acos                          add_months                    approxCountDistinct           array                         array_contains                asInstanceOf                  
            asc                           ascii                         asin                          atan                          atan2                         avg                           base64                        
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
            |           9|
            +------------+



            Let's show order_items data:
            >>> orderItems.show()
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

1. Get revenue for each order_id from order items   

            Now, to get revenue per product "2": 
            (with SQL syntax)
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

            >>> from pyspark.sql.functions import round
            >>> orderItems. \
            ...    filter('order_item_order_id =2'). \
            ...    select(round(sum('order_item_subtotal'),2)). \
            ...    show()
            +----------------------------------+
            |round(sum(order_item_subtotal), 2)|
            +----------------------------------+
            |                            579.98|
            +----------------------------------+
```


To group by a particular field, we must use the groupBy function before the aggregate function:
Let's get revenue per each orderId. Whenever we heard "for each" or "per key", we have to use the groupBy function and after the groupBy functionwe can perform the operations.

```python

            >>> orderItems. \
            ... groupBy('order_item_order_id') .\
            ... sum('order_item_subtotal') .\
            ... show()
            +-------------------+------------------------+
            |order_item_order_id|sum(order_item_subtotal)|
            +-------------------+------------------------+
            |                148|      479.99000549316406|
            |                463|       829.9200096130371|
            |                471|      169.98000717163086|
            |                496|        441.950008392334|
            |               1088|      249.97000885009766|
            |               1580|      299.95001220703125|
            |               1591|       439.8599967956543|
            |               1645|       1509.790023803711|
            |               2366|       299.9700012207031|
            |               2659|       724.9100151062012|
            |               2866|        569.960018157959|
            |               3175|      209.97000122070312|
            |               3749|      143.97000122070312|
            |               3794|      299.95001220703125|
            |               3918|       829.9300155639648|
            |               3997|       579.9500122070312|
            |               4101|      129.99000549316406|
            |               4519|        79.9800033569336|
            |               4818|       399.9800109863281|
            |               4900|       179.9700050354004|
            +-------------------+------------------------+
            only showing top 20 rows
```
As we can see, the sum of order revenue has not an intuitive name. For that purpose, we will create an alias:

```python

            orderItems. \
            ... groupBy('order_item_order_id') .\
            ... sum('order_item_subtotal') .\    -----> we canot use the alias function then, because it will create an alias for the hole dataset. For that reason, to create an alias we have to use the agg function.

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
            |                148|      479.99000549316406|
            |                463|       829.9200096130371|
            |                471|      169.98000717163086|
            |                496|        441.950008392334|
            |               1088|      249.97000885009766|
            |               1580|      299.95001220703125|
            |               1591|       439.8599967956543|
            |               1645|       1509.790023803711|
            |               2366|       299.9700012207031|
            |               2659|       724.9100151062012|
            |               2866|        569.960018157959|
            |               3175|      209.97000122070312|
            |               3749|      143.97000122070312|
            |               3794|      299.95001220703125|
            |               3918|       829.9300155639648|
            |               3997|       579.9500122070312|
            |               4101|      129.99000549316406|
            |               4519|        79.9800033569336|
            |               4818|       399.9800109863281|
            |               4900|       179.9700050354004|
            +-------------------+------------------------+
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
            |                148|479.99000549316406|
            |                463| 829.9200096130371|
            |                471|169.98000717163086|
            |                496|  441.950008392334|
            |               1088|249.97000885009766|
            |               1580|299.95001220703125|
            |               1591| 439.8599967956543|
            |               1645| 1509.790023803711|
            |               2366| 299.9700012207031|
            |               2659| 724.9100151062012|
            |               2866|  569.960018157959|
            |               3175|209.97000122070312|
            |               3749|143.97000122070312|
            |               3794|299.95001220703125|
            |               3918| 829.9300155639648|
            |               3997| 579.9500122070312|
            |               4101|129.99000549316406|
            |               4519|  79.9800033569336|
            |               4818| 399.9800109863281|
            |               4900| 179.9700050354004|
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
            |                148|       479.99|
            |                463|       829.92|
            |                471|       169.98|
            |                496|       441.95|
            |               1088|       249.97|
            |               1580|       299.95|
            |               1591|       439.86|
            |               1645|      1509.79|
            |               2366|       299.97|
            |               2659|       724.91|
            |               2866|       569.96|
            |               3175|       209.97|
            |               3749|       143.97|
            |               3794|       299.95|
            |               3918|       829.93|
            |               3997|       579.95|
            |               4101|       129.99|
            |               4519|        79.98|
            |               4818|       399.98|
            |               4900|       179.97|
            +-------------------+-------------+
            only showing top 20 rows
```
1. Get count by status from orders
```python

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


In order to get the distinct order status values :
```python

            orders.select('order_status').distinct().show()
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
As we can see, we don0t have any alias for the count column. For that reason, we must use the agg function:
```python

            >>> from pyspark.sql.functions import count
            >>> orders.groupBy('order_status').agg(count('order_status').alias('status_count')).show()
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
3. Get daily product revenue (order_date and order_item_product_id are part of keys, order_item_subtotal is used for aggregation).We have to group the data by date (daily revenue) and by product_id.
Firstly, to perform aggregation by those two fields we have to join both tables.
If we preview the data we see:
```python

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
In order table we will need the "order_date" column.
In order_items table we will need to take order_item_product_id 
```python

            column as well as order_item_subtotal
            >>> orderItems.show()
            +-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
            |order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
            +-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
            |            1|                  1|                  957|                1.0|             299.98|                  299.98|
            |            2|                  2|                 1073|                1.0|             199.99|                  199.99|
            |            3|                  2|                  502|                5.0|              250.0|                    50.0|
            |            4|                  2|                  403|                1.0|             129.99|                  129.99|
            |            5|                  4|                  897|                2.0|              49.98|                   24.99|
            |            6|                  4|                  365|                5.0|             299.95|                   59.99|
            |            7|                  4|                  502|                3.0|              150.0|                    50.0|
            |            8|                  4|                 1014|                4.0|             199.92|                   49.98|
            |            9|                  5|                  957|                1.0|             299.98|                  299.98|
            |           10|                  5|                  365|                5.0|             299.95|                   59.99|
            |           11|                  5|                 1014|                2.0|              99.96|                   49.98|
            |           12|                  5|                  957|                1.0|             299.98|                  299.98|
            |           13|                  5|                  403|                1.0|             129.99|                  129.99|
            |           14|                  7|                 1073|                1.0|             199.99|                  199.99|
            |           15|                  7|                  957|                1.0|             299.98|                  299.98|
            |           16|                  7|                  926|                5.0|              79.95|                   15.99|
            |           17|                  8|                  365|                3.0|             179.97|                   59.99|
            |           18|                  8|                  365|                5.0|             299.95|                   59.99|
            |           19|                  8|                 1014|                4.0|             199.92|                   49.98|
            |           20|                  8|                  502|                1.0|               50.0|                    50.0|
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
            |order_id|          order_date|order_customer_id|   order_status|order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
            +--------+--------------------+-----------------+---------------+-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
            |       1|2013-07-25 00:00:...|            11599|         CLOSED|            1|                  1|                  957|                1.0|             299.98|                  299.98|
            |       2|2013-07-25 00:00:...|              256|PENDING_PAYMENT|            2|                  2|                 1073|                1.0|             199.99|                  199.99|
            |       2|2013-07-25 00:00:...|              256|PENDING_PAYMENT|            3|                  2|                  502|                5.0|              250.0|                    50.0|
            |       2|2013-07-25 00:00:...|              256|PENDING_PAYMENT|            4|                  2|                  403|                1.0|             129.99|                  129.99|
            |       4|2013-07-25 00:00:...|             8827|         CLOSED|            5|                  4|                  897|                2.0|              49.98|                   24.99|
            |       4|2013-07-25 00:00:...|             8827|         CLOSED|            6|                  4|                  365|                5.0|             299.95|                   59.99|
            |       4|2013-07-25 00:00:...|             8827|         CLOSED|            7|                  4|                  502|                3.0|              150.0|                    50.0|
            |       4|2013-07-25 00:00:...|             8827|         CLOSED|            8|                  4|                 1014|                4.0|             199.92|                   49.98|
            |       5|2013-07-25 00:00:...|            11318|       COMPLETE|            9|                  5|                  957|                1.0|             299.98|                  299.98|
            |       5|2013-07-25 00:00:...|            11318|       COMPLETE|           10|                  5|                  365|                5.0|             299.95|                   59.99|
            |       5|2013-07-25 00:00:...|            11318|       COMPLETE|           11|                  5|                 1014|                2.0|              99.96|                   49.98|
            |       5|2013-07-25 00:00:...|            11318|       COMPLETE|           12|                  5|                  957|                1.0|             299.98|                  299.98|
            |       5|2013-07-25 00:00:...|            11318|       COMPLETE|           13|                  5|                  403|                1.0|             129.99|                  129.99|
            |       7|2013-07-25 00:00:...|             4530|       COMPLETE|           14|                  7|                 1073|                1.0|             199.99|                  199.99|
            |       7|2013-07-25 00:00:...|             4530|       COMPLETE|           15|                  7|                  957|                1.0|             299.98|                  299.98|
            |       7|2013-07-25 00:00:...|             4530|       COMPLETE|           16|                  7|                  926|                5.0|              79.95|                   15.99|
            |       8|2013-07-25 00:00:...|             2911|     PROCESSING|           17|                  8|                  365|                3.0|             179.97|                   59.99|
            |       8|2013-07-25 00:00:...|             2911|     PROCESSING|           18|                  8|                  365|                5.0|             299.95|                   59.99|
            |       8|2013-07-25 00:00:...|             2911|     PROCESSING|           19|                  8|                 1014|                4.0|             199.92|                   49.98|
            |       8|2013-07-25 00:00:...|             2911|     PROCESSING|           20|                  8|                  502|                1.0|               50.0|                    50.0|
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
            |          order_date|order_item_product_id|   product_revenue|
            +--------------------+---------------------+------------------+
            |2013-07-27 00:00:...|                  703| 99.95000076293945|
            |2013-07-29 00:00:...|                  793|44.970001220703125|
            |2013-08-04 00:00:...|                  825| 95.97000122070312|
            |2013-08-06 00:00:...|                   93|24.989999771118164|
            |2013-08-12 00:00:...|                  627| 5358.660064697266|
            |2013-08-15 00:00:...|                  926|15.989999771118164|
            |2013-08-17 00:00:...|                  116| 224.9499969482422|
            |2013-09-04 00:00:...|                  957| 7499.500274658203|
            |2013-09-07 00:00:...|                  235|104.97000122070312|
            |2013-09-17 00:00:...|                  792| 89.93999671936035|
            |2013-09-25 00:00:...|                   44| 359.9400100708008|
            |2013-09-27 00:00:...|                  276|31.989999771118164|
            |2013-09-28 00:00:...|                  572|119.97000122070312|
            |2013-10-04 00:00:...|                  792|44.970001220703125|
            |2013-10-05 00:00:...|                  886|24.989999771118164|
            |2013-10-14 00:00:...|                  835|31.989999771118164|
            |2013-10-16 00:00:...|                  835|31.989999771118164|
            |2013-10-19 00:00:...|                 1004|18399.080505371094|
            |2013-11-06 00:00:...|                  502|            9750.0|
            |2013-11-12 00:00:...|                  282| 63.97999954223633|
            +--------------------+---------------------+------------------+
            only showing top 20 rows


            >>> ordersJoin. \
            ...     groupBy('order_date', 'order_item_product_id'). \
            ...     agg(round(sum('order_item_subtotal'),2).alias('product_revenue')).show()

            +--------------------+---------------------+---------------+
            |          order_date|order_item_product_id|product_revenue|
            +--------------------+---------------------+---------------+
            |2013-07-27 00:00:...|                  703|          99.95|
            |2013-07-29 00:00:...|                  793|          44.97|
            |2013-08-04 00:00:...|                  825|          95.97|
            |2013-08-06 00:00:...|                   93|          24.99|
            |2013-08-12 00:00:...|                  627|        5358.66|
            |2013-08-15 00:00:...|                  926|          15.99|
            |2013-08-17 00:00:...|                  116|         224.95|
            |2013-09-04 00:00:...|                  957|         7499.5|
            |2013-09-07 00:00:...|                  235|         104.97|
            |2013-09-17 00:00:...|                  792|          89.94|
            |2013-09-25 00:00:...|                   44|         359.94|
            |2013-09-27 00:00:...|                  276|          31.99|
            |2013-09-28 00:00:...|                  572|         119.97|
            |2013-10-04 00:00:...|                  792|          44.97|
            |2013-10-05 00:00:...|                  886|          24.99|
            |2013-10-14 00:00:...|                  835|          31.99|
            |2013-10-16 00:00:...|                  835|          31.99|
            |2013-10-19 00:00:...|                 1004|       18399.08|
            |2013-11-06 00:00:...|                  502|         9750.0|
            |2013-11-12 00:00:...|                  282|          63.98|
            +--------------------+---------------------+---------------+
            only showing top 20 rows
```

### SORTING DATA
We can sort the data coming from an aggregation:

+ sort or orderBy can be used to sort the data globally.
+ we can get the help by doing help(orders.sort) and help(orders.orderBy)

```python

                        orders in ascending order by date
                        >>> orders.sort('order_date').show()
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

Now, if we want to order , in ascending order, by two columns, we have to perform a composite "orderBy":
```python

                        >>> orders.sort('order_date','order_customer_id').show()
                        +--------+--------------------+-----------------+---------------+
                        |order_id|          order_date|order_customer_id|   order_status|
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

                        >>> orders.sort(['order_date', 'order_customer_id'], ascending = [0,1]).show()
                        +--------+--------------------+-----------------+---------------+
                        |order_id|          order_date|order_customer_id|   order_status|
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
We can do the same in the following way:
```python

                        >>> orders.sort('order_date', orders.order_customer_id.desc()).show()
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
+ By default, data will be sorted in ascending order.
+ We can change the order by using the orderBy using desc function.
  + At times, we might not want to sort the data globally. Instead, we might want to sort the data within a group. In that case, we can use sortWithinPartitions (for example to sort the stores by revenue within each state):
  Example:
  Say we want to order the data by date and then, we want to order the data by order_customer_id
```python

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



                            >>> orders.sortWithinPartitions('order_date','order_customer_id').show()
                            +--------+--------------------+-----------------+---------------+
                            |order_id|          order_date|order_customer_id|   order_status|
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
                            only showing top 20 rows
```

The difference between sort and sortWithinPartitions is that sort function sorts the data globally and sortWithinPartitions does not
**Examples**
Sort orders by status.            
```python

                        >>> orders.sort('order_status').show()
                        +--------+--------------------+-----------------+------------+
                        |order_id|          order_date|order_customer_id|order_status|
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


**Example**
sort orders by date and then by status (in ascending way both fields).            
```python

                        >>> orders.sort('order_date','order_status').show()
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

                        >>> orders.sort(orders.order_date.desc(),'order_status').show()
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

**Example**
sort orderItems by orde_item_order_id and order_item_subtotal descending
```python

                        >>> orderItems.sort('order_item_order_id', orderItems.order_item_subtotal.desc()).show()
                        +-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
                        |order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
                        +-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
                        |            1|                  1|                  957|                1.0|             299.98|                  299.98|
                        |            3|                  2|                  502|                5.0|              250.0|                    50.0|
                        |            2|                  2|                 1073|                1.0|             199.99|                  199.99|
                        |            4|                  2|                  403|                1.0|             129.99|                  129.99|
                        |            6|                  4|                  365|                5.0|             299.95|                   59.99|
                        |            8|                  4|                 1014|                4.0|             199.92|                   49.98|
                        |            7|                  4|                  502|                3.0|              150.0|                    50.0|
                        |            5|                  4|                  897|                2.0|              49.98|                   24.99|
                        |            9|                  5|                  957|                1.0|             299.98|                  299.98|
                        |           12|                  5|                  957|                1.0|             299.98|                  299.98|
                        |           10|                  5|                  365|                5.0|             299.95|                   59.99|
                        |           13|                  5|                  403|                1.0|             129.99|                  129.99|
                        |           11|                  5|                 1014|                2.0|              99.96|                   49.98|
                        |           15|                  7|                  957|                1.0|             299.98|                  299.98|
                        |           14|                  7|                 1073|                1.0|             199.99|                  199.99|
                        |           16|                  7|                  926|                5.0|              79.95|                   15.99|
                        |           18|                  8|                  365|                5.0|             299.95|                   59.99|
                        |           19|                  8|                 1014|                4.0|             199.92|                   49.98|
                        |           17|                  8|                  365|                3.0|             179.97|                   59.99|
                        |           20|                  8|                  502|                1.0|               50.0|                    50.0|
                        +-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
                        only showing top 20 rows

```
Or we can do the same in the following way:
```python

                        >>> orderItems.sort(['order_item_order_id', 'order_item_subtotal'],ascending = [1,0]).show()
                        +-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
                        |order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price|
                        +-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
                        |            1|                  1|                  957|                1.0|             299.98|                  299.98|
                        |            3|                  2|                  502|                5.0|              250.0|                    50.0|
                        |            2|                  2|                 1073|                1.0|             199.99|                  199.99|
                        |            4|                  2|                  403|                1.0|             129.99|                  129.99|
                        |            6|                  4|                  365|                5.0|             299.95|                   59.99|
                        |            8|                  4|                 1014|                4.0|             199.92|                   49.98|
                        |            7|                  4|                  502|                3.0|              150.0|                    50.0|
                        |            5|                  4|                  897|                2.0|              49.98|                   24.99|
                        |            9|                  5|                  957|                1.0|             299.98|                  299.98|
                        |           12|                  5|                  957|                1.0|             299.98|                  299.98|
                        |           10|                  5|                  365|                5.0|             299.95|                   59.99|
                        |           13|                  5|                  403|                1.0|             129.99|                  129.99|
                        |           11|                  5|                 1014|                2.0|              99.96|                   49.98|
                        |           15|                  7|                  957|                1.0|             299.98|                  299.98|
                        |           14|                  7|                 1073|                1.0|             199.99|                  199.99|
                        |           16|                  7|                  926|                5.0|              79.95|                   15.99|
                        |           18|                  8|                  365|                5.0|             299.95|                   59.99|
                        |           19|                  8|                 1014|                4.0|             199.92|                   49.98|
                        |           17|                  8|                  365|                3.0|             179.97|                   59.99|
                        |           20|                  8|                  502|                1.0|               50.0|                    50.0|
                        +-------------+-------------------+---------------------+-------------------+-------------------+------------------------+
                        only showing top 20 rows
```


**Example**
Take daily product revenue data and sort in ascending order by date and then descending order by revenue:
```python

                        from pyspark.sql.functions import sum, round

                        >>> dailyRevenue = orders.where('order_status in ("COMPLETE", "CLOSED")').\
                        ...     join(orderItems, orders.order_id == orderItems.order_item_order_id).\
                        ...     groupBy(orders.order_date, orderItems.order_item_product_id).\
                        ...     agg(round(sum(orderItems.order_item_subtotal),2).alias('revenue'))
                        dailyRevenue.show()
                        +--------------------+---------------------+-------+
                        |          order_date|order_item_product_id|revenue|
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

                        >>> dailyRevenue.sort('order_date', dailyRevenue.revenue.desc()).show()
                        +--------------------+---------------------+-------+                            
                        |          order_date|order_item_product_id|revenue|
                        +--------------------+---------------------+-------+
                        |2013-07-25 00:00:...|                 1004|5599.72|
                        |2013-07-25 00:00:...|                  191|5099.49|
                        |2013-07-25 00:00:...|                  957| 4499.7|
                        |2013-07-25 00:00:...|                  365|3359.44|
                        |2013-07-25 00:00:...|                 1073|2999.85|
                        |2013-07-25 00:00:...|                 1014|2798.88|
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

                        >>> dailyRevenue.sort(['order_date','revenue'], ascending = [1,0]).show()
                        +--------------------+---------------------+-------+
                        |          order_date|order_item_product_id|revenue|
                        +--------------------+---------------------+-------+
                        |2013-07-25 00:00:...|                 1004|5599.72|
                        |2013-07-25 00:00:...|                  191|5099.49|
                        |2013-07-25 00:00:...|                  957| 4499.7|
                        |2013-07-25 00:00:...|                  365|3359.44|
                        |2013-07-25 00:00:...|                 1073|2999.85|
                        |2013-07-25 00:00:...|                 1014|2798.88|
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


## APACHE SPARK 2.X
  
### PROCESSING DATA USING DATAFRAMES - WINDOW FUNCTIONS

Why using "PARTITION BY" in spite of groupBy? Because when we use groupBy there is a limitation of either use the select field as part  the group clause
(when data is grouped by a field, that field must takes part of the fields selected) or using an aggregation function such as sum, min, avg on a numeric field.
```python

        >>> employeesPath = '/public/hr_db/employees'
        >>> employees = spark. \
        ...     read. \
        ...     format('csv'). \
        ...     option('sep', '\t'). \
        ...     schema('''employee_id INT, 
        ...               first_name STRING, 
        ...               last_name STRING, 
        ...               email STRING,
        ...               phone_number STRING, 
        ...               hire_date STRING, 
        ...               job_id STRING, 
        ...               salary FLOAT,
        ...               commission_pct STRING,
        ...               manager_id STRING, 
        ...               department_id STRING
        ...             '''). \
        ...     load(employeesPath)

            >>> employees.select('employee_id', 'department_id', 'salary').show()
            +-----------+-------------+-------+                                             
            |employee_id|department_id| salary|
            +-----------+-------------+-------+
            |        127|           50| 2400.0|
            |        128|           50| 2200.0|
            |        129|           50| 3300.0|
            |        130|           50| 2800.0|
            |        131|           50| 2500.0|
            |        132|           50| 2100.0|
            |        133|           50| 3300.0|
            |        134|           50| 2900.0|
            |        135|           50| 2400.0|
            |        136|           50| 2200.0|
            |        137|           50| 3600.0|
            |        138|           50| 3200.0|
            |        139|           50| 2700.0|
            |        140|           50| 2500.0|
            |        141|           50| 3500.0|
            |        142|           50| 3100.0|
            |        143|           50| 2600.0|
            |        144|           50| 2500.0|
            |        145|           80|14000.0|
            |        146|           80|13500.0|
            +-----------+-------------+-------+
            only showing top 20 rows
```

**Problem**:
Get salary by department
```python

            >>> employees.select('employee_id', 'department_id', 'salary').groupBy('department_id'). \
            ...     sum('salary').show()
            +-------------+-----------+
            |department_id|sum(salary)|
            +-------------+-----------+
            |           30|    24900.0|
            |          110|    20300.0|
            |          100|    51600.0|
            |           70|    10000.0|
            |           90|    58000.0|
            |           60|    28800.0|
            |           40|     6500.0|
            |           20|    19000.0|
            |           10|     4400.0|
            |           80|   304500.0|
            |         null|     7000.0|
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
            |           30|       24900.0|
            |          110|       20300.0|
            |          100|       51600.0|
            |           70|       10000.0|
            |           90|       58000.0|
            |           60|       28800.0|
            |           40|        6500.0|
            |           20|       19000.0|
            |           10|        4400.0|
            |           80|      304500.0|
            |         null|        7000.0|
            |           50|      156400.0|
            +-------------+--------------+
```

However, if we want to compare a salary of an exployee_id in comparison with its deparment salary and understand which is the percentage salary of that employee, we have to self-join with exployees and take a few steps further.
However, we can use a windowing function to simplify the solution and make it much more efficient.
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
            |            1|                  1|                  957|                1.0|             299.98|                  299.98|
            |            2|                  2|                 1073|                1.0|             199.99|                  199.99|
            |            3|                  2|                  502|                5.0|              250.0|                    50.0|
            |            4|                  2|                  403|                1.0|             129.99|                  129.99|
            |            5|                  4|                  897|                2.0|              49.98|                   24.99|
            |            6|                  4|                  365|                5.0|             299.95|                   59.99|
            |            7|                  4|                  502|                3.0|              150.0|                    50.0|
            |            8|                  4|                 1014|                4.0|             199.92|                   49.98|
            |            9|                  5|                  957|                1.0|             299.98|                  299.98|
            |           10|                  5|                  365|                5.0|             299.95|                   59.99|
            |           11|                  5|                 1014|                2.0|              99.96|                   49.98|
            |           12|                  5|                  957|                1.0|             299.98|                  299.98|
            |           13|                  5|                  403|                1.0|             129.99|                  129.99|
            |           14|                  7|                 1073|                1.0|             199.99|                  199.99|
            |           15|                  7|                  957|                1.0|             299.98|                  299.98|
            |           16|                  7|                  926|                5.0|              79.95|                   15.99|
            |           17|                  8|                  365|                3.0|             179.97|                   59.99|
            |           18|                  8|                  365|                5.0|             299.95|                   59.99|
            |           19|                  8|                 1014|                4.0|             199.92|                   49.98|
            |           20|                  8|                  502|                1.0|               50.0|                    50.0|
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
            |          348|                148|                  502|                2.0|              100.0|                    50.0|479.99000549316406|
            |          349|                148|                  502|                5.0|              250.0|                    50.0|479.99000549316406|
            |          350|                148|                  403|                1.0|             129.99|                  129.99|479.99000549316406|
            |         1129|                463|                  365|                4.0|             239.96|                   59.99| 829.9200096130371|
            |         1130|                463|                  502|                5.0|              250.0|                    50.0| 829.9200096130371|
            |         1131|                463|                  627|                1.0|              39.99|                   39.99| 829.9200096130371|
            |         1132|                463|                  191|                3.0|             299.97|                   99.99| 829.9200096130371|
            |         1153|                471|                  627|                1.0|              39.99|                   39.99|169.98000717163086|
            |         1154|                471|                  403|                1.0|             129.99|                  129.99|169.98000717163086|
            |         1223|                496|                  365|                1.0|              59.99|                   59.99|  441.950008392334|
            |         1224|                496|                  502|                3.0|              150.0|                    50.0|  441.950008392334|
            |         1225|                496|                  821|                1.0|              51.99|                   51.99|  441.950008392334|
            |         1226|                496|                  403|                1.0|             129.99|                  129.99|  441.950008392334|
            |         1227|                496|                 1014|                1.0|              49.98|                   49.98|  441.950008392334|
            |         2703|               1088|                  403|                1.0|             129.99|                  129.99|249.97000885009766|
            |         2704|               1088|                  365|                2.0|             119.98|                   59.99|249.97000885009766|
            |         3944|               1580|                   44|                5.0|             299.95|                   59.99|299.95001220703125|
            |         3968|               1591|                  627|                5.0|             199.95|                   39.99| 439.8599967956543|
            |         3969|               1591|                  627|                1.0|              39.99|                   39.99| 439.8599967956543|
            |         3970|               1591|                 1014|                4.0|             199.92|                   49.98| 439.8599967956543|
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
            |          348|                148|              100.0|       479.99|
            |          349|                148|              250.0|       479.99|
            |          350|                148|             129.99|       479.99|
            |         1129|                463|             239.96|       829.92|
            |         1130|                463|              250.0|       829.92|
            |         1131|                463|              39.99|       829.92|
            |         1132|                463|             299.97|       829.92|
            |         1153|                471|              39.99|       169.98|
            |         1154|                471|             129.99|       169.98|
            |         1223|                496|              59.99|       441.95|
            |         1224|                496|              150.0|       441.95|
            |         1225|                496|              51.99|       441.95|
            |         1226|                496|             129.99|       441.95|
            |         1227|                496|              49.98|       441.95|
            |         2703|               1088|             129.99|       249.97|
            |         2704|               1088|             119.98|       249.97|
            |         3944|               1580|             299.95|       299.95|
            |         3968|               1591|             199.95|       439.86|
            |         3969|               1591|              39.99|       439.86|
            |         3970|               1591|             199.92|       439.86|   
            +-------------+-------------------+-------------------+-------------+
            only showing top 20 rows
```

**Problem**: Get top N products per day

This is nothing but a ranking use case, so we will use the rank function which comes as part of the Window. 
```python

            orders = spark.read.format('csv').\
            schema('order_id int, order_date string, order_customer_id int, order_status string').\
            load('/public/retail_db/orders')


            from pyspark.sql.functions import sum, round
            We implement the daily revenue sorted: 
            dailyRevenue = orders.where('order_status in ("COMPLETE", "CLOSED")').\
                 join(orderItems, orders.order_id == orderItems.order_item_order_id).\
                 groupBy(orders.order_date, orderItems.order_item_product_id).\
                 agg(round(sum(orderItems.order_item_subtotal),2).alias('revenue'))


            dailyRevenueSorted = dailyRevenue.sort('order_date', dailyRevenue.revenue.desc())

            >>> dailyRevenueSorted.show(100)
            +--------------------+---------------------+--------+
            |          order_date|order_item_product_id| revenue|
            +--------------------+---------------------+--------+
            |2013-07-25 00:00:...|                 1004| 5599.72|
            |2013-07-25 00:00:...|                  191| 5099.49|
            |2013-07-25 00:00:...|                  957|  4499.7|
            |2013-07-25 00:00:...|                  365| 3359.44|
            |2013-07-25 00:00:...|                 1073| 2999.85|
            |2013-07-25 00:00:...|                 1014| 2798.88|
            |2013-07-25 00:00:...|                  403| 1949.85|
            |2013-07-25 00:00:...|                  502|  1650.0|
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
```


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
                    |order_id|          order_date|order_customer_id|   order_status|daily_count|
                    +--------+--------------------+-----------------+---------------+-----------+
                    |    3378|2013-08-13 00:00:|             3155|     PROCESSING|         73|
                    |    3379|2013-08-13 00:00:|             5437|       COMPLETE|         73|
                    |    3380|2013-08-13 00:00:|             3519|         CLOSED|         73|
                    |    3381|2013-08-13 00:00:|            10023|        ON_HOLD|         73|
                    |    3382|2013-08-13 00:00:|             7856|        PENDING|         73|
                    |    3383|2013-08-13 00:00:|             1523|PENDING_PAYMENT|         73|
                    |    3384|2013-08-13 00:00:|            12398|PENDING_PAYMENT|         73|
                    |    3385|2013-08-13 00:00:|              132|       COMPLETE|         73|
                    |    3386|2013-08-13 00:00:|             2128|        PENDING|         73|
                    |    3387|2013-08-13 00:00:|             2735|       COMPLETE|         73|
                    |    3388|2013-08-13 00:00:|            12319|       COMPLETE|         73|
                    |    3389|2013-08-13 00:00:|             7024|       COMPLETE|         73|
                    |    3390|2013-08-13 00:00:|             5012|         CLOSED|         73|
                    |    3391|2013-08-13 00:00:|            11076|     PROCESSING|         73|
                    |    3392|2013-08-13 00:00:|             8989|         CLOSED|         73|
                    |    3393|2013-08-13 00:00:|            11247|       COMPLETE|         73|
                    |    3394|2013-08-13 00:00:|              890|PENDING_PAYMENT|         73|
                    |    3395|2013-08-13 00:00:|             1618|        PENDING|         73|
                    |    3396|2013-08-13 00:00:|             9491|PENDING_PAYMENT|         73|
                    |    3397|2013-08-13 00:00:|             6722|PENDING_PAYMENT|         73|
                    +--------+--------------------+-----------------+---------------+-----------+
                    only showing top 20 rows

```

Normally we use:
 + partitionBy for aggregations
 + partitionBy + orderBy for aggregations

### Dataframe operations - Performing aggregations using sum, avg, etc
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
We see that manager_id and department_id fields are both string type. The reason being is because they, both, can have null values.
Out of all those fields, we are interested in salary, which is the field that we are goin to perform aggregations to, and in addition, we want to have employee_id and department_id.
```python

             >>> employees.select('employee_id', 'department_id', 'salary').show()
            +-----------+-------------+-------+
            |employee_id|department_id| salary|
            +-----------+-------------+-------+
            |        127|           50| 2400.0|
            |        128|           50| 2200.0|
            |        129|           50| 3300.0|
            |        130|           50| 2800.0|
            |        131|           50| 2500.0|
            |        132|           50| 2100.0|
            |        133|           50| 3300.0|
            |        134|           50| 2900.0|
            |        135|           50| 2400.0|
            |        136|           50| 2200.0|
            |        137|           50| 3600.0|
            |        138|           50| 3200.0|
            |        139|           50| 2700.0|
            |        140|           50| 2500.0|
            |        141|           50| 3500.0|
            |        142|           50| 3100.0|
            |        143|           50| 2600.0|
            |        144|           50| 2500.0|
            |        145|           80|14000.0|
            |        146|           80|13500.0|
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
            |        114|           30|11000.0|       24900.0|
            |        115|           30| 3100.0|       24900.0|
            |        116|           30| 2900.0|       24900.0|
            |        117|           30| 2800.0|       24900.0|
            |        118|           30| 2600.0|       24900.0|
            |        119|           30| 2500.0|       24900.0|
            |        205|          110|12000.0|       20300.0|
            |        206|          110| 8300.0|       20300.0|
            |        108|          100|12000.0|       51600.0|
            |        109|          100| 9000.0|       51600.0|
            |        110|          100| 8200.0|       51600.0|
            |        111|          100| 7700.0|       51600.0|
            |        112|          100| 7800.0|       51600.0|
            |        113|          100| 6900.0|       51600.0|
            |        204|           70|10000.0|       10000.0|
            |        103|           60| 9000.0|       28800.0|
            |        104|           60| 6000.0|       28800.0|
            |        105|           60| 4800.0|       28800.0|
            |        106|           60| 4800.0|       28800.0|
            |        107|           60| 4200.0|       28800.0|
            +-----------+-------------+-------+--------------+
            only showing top 20 rows
```

**Explanation:**
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
            |        114|           30|11000.0|       24900.0|      2500.0|
            |        115|           30| 3100.0|       24900.0|      2500.0|
            |        116|           30| 2900.0|       24900.0|      2500.0|
            |        117|           30| 2800.0|       24900.0|      2500.0|
            |        118|           30| 2600.0|       24900.0|      2500.0|
            |        119|           30| 2500.0|       24900.0|      2500.0|
            |        205|          110|12000.0|       20300.0|      8300.0|
            |        206|          110| 8300.0|       20300.0|      8300.0|
            |        108|          100|12000.0|       51600.0|      6900.0|
            |        109|          100| 9000.0|       51600.0|      6900.0|
            |        110|          100| 8200.0|       51600.0|      6900.0|
            |        111|          100| 7700.0|       51600.0|      6900.0|
            |        112|          100| 7800.0|       51600.0|      6900.0|
            |        113|          100| 6900.0|       51600.0|      6900.0|
            |        204|           70|10000.0|       10000.0|     10000.0|
            |        103|           60| 9000.0|       28800.0|      4200.0|
            |        104|           60| 6000.0|       28800.0|      4200.0|
            |        105|           60| 4800.0|       28800.0|      4200.0|
            |        106|           60| 4800.0|       28800.0|      4200.0|
            |        107|           60| 4200.0|       28800.0|      4200.0|
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
            |        114|           30|11000.0|       24900.0|      2500.0|       11000.0|        4150.0|
            |        115|           30| 3100.0|       24900.0|      2500.0|       11000.0|        4150.0|
            |        116|           30| 2900.0|       24900.0|      2500.0|       11000.0|        4150.0|
            |        117|           30| 2800.0|       24900.0|      2500.0|       11000.0|        4150.0|
            |        118|           30| 2600.0|       24900.0|      2500.0|       11000.0|        4150.0|
            |        119|           30| 2500.0|       24900.0|      2500.0|       11000.0|        4150.0|
            |        205|          110|12000.0|       20300.0|      8300.0|       12000.0|       10150.0|
            |        206|          110| 8300.0|       20300.0|      8300.0|       12000.0|       10150.0|
            |        108|          100|12000.0|       51600.0|      6900.0|       12000.0|        8600.0|
            |        109|          100| 9000.0|       51600.0|      6900.0|       12000.0|        8600.0|
            |        110|          100| 8200.0|       51600.0|      6900.0|       12000.0|        8600.0|
            |        111|          100| 7700.0|       51600.0|      6900.0|       12000.0|        8600.0|
            |        112|          100| 7800.0|       51600.0|      6900.0|       12000.0|        8600.0|
            |        113|          100| 6900.0|       51600.0|      6900.0|       12000.0|        8600.0|
            |        204|           70|10000.0|       10000.0|     10000.0|       10000.0|       10000.0|
            |        103|           60| 9000.0|       28800.0|      4200.0|        9000.0|        5760.0|
            |        104|           60| 6000.0|       28800.0|      4200.0|        9000.0|        5760.0|
            |        105|           60| 4800.0|       28800.0|      4200.0|        9000.0|        5760.0|
            |        106|           60| 4800.0|       28800.0|      4200.0|        9000.0|        5760.0|
            |        107|           60| 4200.0|       28800.0|      4200.0|        9000.0|        5760.0|
            +-----------+-------------+-------+--------------+------------+--------------+--------------+
            only showing top 20 rows
```
If we want to calculate each salary percentage against total salary of each department:
We have to import the col function because earlier we mentioned that salary_expense was defined as a string because we have to manage null possible values
```python

            >>> from pyspark.sql.functions import col
            >>> employees.select('employee_id', 'department_id', 'salary').\
            ... withColumn('salary_expense', sum('salary').over(spec)).\
            ... withColumn('salary_pct', employees.salary/col('salary_expense')).\
            ... show()
            +-----------+-------------+-------+--------------+-------------------+
            |employee_id|department_id| salary|salary_expense|         salary_pct|
            +-----------+-------------+-------+--------------+-------------------+
            |        114|           30|11000.0|       24900.0|0.44176706827309237|
            |        115|           30| 3100.0|       24900.0|0.12449799196787148|
            |        116|           30| 2900.0|       24900.0|0.11646586345381527|
            |        117|           30| 2800.0|       24900.0|0.11244979919678715|
            |        118|           30| 2600.0|       24900.0|0.10441767068273092|
            |        119|           30| 2500.0|       24900.0|0.10040160642570281|
            |        205|          110|12000.0|       20300.0| 0.5911330049261084|
            |        206|          110| 8300.0|       20300.0| 0.4088669950738916|
            |        108|          100|12000.0|       51600.0|0.23255813953488372|
            |        109|          100| 9000.0|       51600.0| 0.1744186046511628|
            |        110|          100| 8200.0|       51600.0|0.15891472868217055|
            |        111|          100| 7700.0|       51600.0|0.14922480620155038|
            |        112|          100| 7800.0|       51600.0| 0.1511627906976744|
            |        113|          100| 6900.0|       51600.0|0.13372093023255813|
            |        204|           70|10000.0|       10000.0|                1.0|
            |        103|           60| 9000.0|       28800.0|             0.3125|
            |        104|           60| 6000.0|       28800.0|0.20833333333333334|
            |        105|           60| 4800.0|       28800.0|0.16666666666666666|
            |        106|           60| 4800.0|       28800.0|0.16666666666666666|
            |        107|           60| 4200.0|       28800.0|0.14583333333333334|
            +-----------+-------------+-------+--------------+-------------------+
            only showing top 20 rows




            >>> employees.select('employee_id', 'department_id', 'salary').\
            ...  withColumn('salary_expense', sum('salary').over(spec)).\
            ...  withColumn('salary_pct',round(( employees.salary/col('salary_expense'))*100,2)).\
            ... show()

            +-----------+-------------+-------+--------------+----------+
            |employee_id|department_id| salary|salary_expense|salary_pct|
            +-----------+-------------+-------+--------------+----------+
            |        114|           30|11000.0|       24900.0|     44.18|
            |        115|           30| 3100.0|       24900.0|     12.45|
            |        116|           30| 2900.0|       24900.0|     11.65|
            |        117|           30| 2800.0|       24900.0|     11.24|
            |        118|           30| 2600.0|       24900.0|     10.44|
            |        119|           30| 2500.0|       24900.0|     10.04|
            |        205|          110|12000.0|       20300.0|     59.11|
            |        206|          110| 8300.0|       20300.0|     40.89|
            |        108|          100|12000.0|       51600.0|     23.26|
            |        109|          100| 9000.0|       51600.0|     17.44|
            |        110|          100| 8200.0|       51600.0|     15.89|
            |        111|          100| 7700.0|       51600.0|     14.92|
            |        112|          100| 7800.0|       51600.0|     15.12|
            |        113|          100| 6900.0|       51600.0|     13.37|
            |        204|           70|10000.0|       10000.0|     100.0|
            |        103|           60| 9000.0|       28800.0|     31.25|
            |        104|           60| 6000.0|       28800.0|     20.83|
            |        105|           60| 4800.0|       28800.0|     16.67|
            |        106|           60| 4800.0|       28800.0|     16.67|
            |        107|           60| 4200.0|       28800.0|     14.58|
            +-----------+-------------+-------+--------------+----------+
            only showing top 20 rows

```



### Dataframe operations: time series functions such us Lead, Lag, etc
Let us see details about windowing functions where data is partitioned by a key (such as department) and then sorted by some other key (such as hire date)
We have functions such as lead, lag, first, last, etc.
For most of those functions we have to use partitionBy and then, orderBy some other key.
Those 4 functions can take any field within the group. They might not select the same fields which were taken as part of the orderBy.
Example:
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
                            |        127|     James|    Landry| JLANDRY|      650.124.1334|1999-01-14|ST_CLERK| 2400.0|          null|       120|           50|
                            |        128|    Steven|    Markle| SMARKLE|      650.124.1434|2000-03-08|ST_CLERK| 2200.0|          null|       120|           50|
                            |        129|     Laura|    Bissot| LBISSOT|      650.124.5234|1997-08-20|ST_CLERK| 3300.0|          null|       121|           50|
                            |        130|     Mozhe|  Atkinson|MATKINSO|      650.124.6234|1997-10-30|ST_CLERK| 2800.0|          null|       121|           50|
                            |        131|     James|    Marlow| JAMRLOW|      650.124.7234|1997-02-16|ST_CLERK| 2500.0|          null|       121|           50|
                            |        132|        TJ|     Olson| TJOLSON|      650.124.8234|1999-04-10|ST_CLERK| 2100.0|          null|       121|           50|
                            |        133|     Jason|    Mallin| JMALLIN|      650.127.1934|1996-06-14|ST_CLERK| 3300.0|          null|       122|           50|
                            |        134|   Michael|    Rogers| MROGERS|      650.127.1834|1998-08-26|ST_CLERK| 2900.0|          null|       122|           50|
                            |        135|        Ki|       Gee|    KGEE|      650.127.1734|1999-12-12|ST_CLERK| 2400.0|          null|       122|           50|
                            |        136|     Hazel|Philtanker|HPHILTAN|      650.127.1634|2000-02-06|ST_CLERK| 2200.0|          null|       122|           50|
                            |        137|    Renske|    Ladwig| RLADWIG|      650.121.1234|1995-07-14|ST_CLERK| 3600.0|          null|       123|           50|
                            |        138|   Stephen|    Stiles| SSTILES|      650.121.2034|1997-10-26|ST_CLERK| 3200.0|          null|       123|           50|
                            |        139|      John|       Seo|    JSEO|      650.121.2019|1998-02-12|ST_CLERK| 2700.0|          null|       123|           50|
                            |        140|    Joshua|     Patel|  JPATEL|      650.121.1834|1998-04-06|ST_CLERK| 2500.0|          null|       123|           50|
                            |        141|    Trenna|      Rajs|   TRAJS|      650.121.8009|1995-10-17|ST_CLERK| 3500.0|          null|       124|           50|
                            |        142|    Curtis|    Davies| CDAVIES|      650.121.2994|1997-01-29|ST_CLERK| 3100.0|          null|       124|           50|
                            |        143|   Randall|     Matos|  RMATOS|      650.121.2874|1998-03-15|ST_CLERK| 2600.0|          null|       124|           50|
                            |        144|     Peter|    Vargas| PVARGAS|      650.121.2004|1998-07-09|ST_CLERK| 2500.0|          null|       124|           50|
                            |        145|      John|   Russell| JRUSSEL|011.44.1344.429268|1996-10-01|  SA_MAN|14000.0|          0.40|       100|           80|
                            |        146|     Karen|  Partners|KPARTNER|011.44.1344.467268|1997-01-05|  SA_MAN|13500.0|          0.30|       100|           80|
                            +-----------+----------+----------+--------+------------------+----------+--------+-------+--------------+----------+-------------+
                            only showing top 20 rows
                    
                            spark.sonf.set('sparlk.sql.shuffle.partitions','2')  ----> to make it partition data in just two parts        
```
We are going to use just three fields from employees:
```python

                            >>> employees.select('employee_id','department_id', 'salary').sort('department_id').show()
                            +-----------+-------------+-------+
                            |employee_id|department_id| salary|
                            +-----------+-------------+-------+
                            |        200|           10| 4400.0|
                            |        111|          100| 7700.0|
                            |        109|          100| 9000.0|
                            |        112|          100| 7800.0|
                            |        108|          100|12000.0|
                            |        113|          100| 6900.0|
                            |        110|          100| 8200.0|
                            |        205|          110|12000.0|
                            |        206|          110| 8300.0|
                            |        201|           20|13000.0|
                            |        202|           20| 6000.0|
                            |        115|           30| 3100.0|
                            |        119|           30| 2500.0|
                            |        114|           30|11000.0|
                            |        118|           30| 2600.0|
                            |        116|           30| 2900.0|
                            |        117|           30| 2800.0|
                            |        203|           40| 6500.0|
                            |        186|           50| 3400.0|
                            |        187|           50| 3000.0|
                            +-----------+-------------+-------+
                            only showing top 20 rows
```
Considering these rows:
```python

                            +-----------+-------------+-------+
                            |employee_id|department_id| salary|
                            +-----------+-------------+-------+
                            |        111|          100| 7700.0|
                            |        109|          100| 9000.0|
                            |        112|          100| 7800.0|
                            |        108|          100|12000.0|
                            |        113|          100| 6900.0|
                            |        110|          100| 8200.0|
```


**Problem**:
 
What we want to know is to get the highest paid employee, the second highest  paid and the difference between current salary and the next best salary paid employee. 

                            + Lag: if we want to get previous element based on a criteria
                            + Lead: if we want to have the next element based on a criteria.

```python

                                from pyspark.sql.window import *
                                spec = window.\
                                ... partitionBy('department_id'). \  ---> we are trying to get the next better paid within each department
                                    orderBy(employees.salary.desc()) ---> we want to order employees based on salary

                                from pyspark.sql.functions import lead
                                >>> employees.select('employee_id','department_id', 'salary').\
                                    withColumn('next_employee_id'), lead('employee_id').over(spec)).\---> we need to add some more columns (as part of lead we add)'employee_id' since we want to know who is the next better paid
                                    sort('department_id', employees.salary.desc()).\
                                    show()

                                    ... show()
                                    +-----------+-------------+-------+----------------+                            
                                    |employee_id|department_id| salary|next_employee_id|
                                    +-----------+-------------+-------+----------------+
                                    |        200|           10| 4400.0|            null|
                                    |        108|          100|12000.0|             109|
                                    |        109|          100| 9000.0|             110|
                                    |        110|          100| 8200.0|             112|
                                    |        112|          100| 7800.0|             111|
                                    |        111|          100| 7700.0|             113|
                                    |        113|          100| 6900.0|            null|
                                    |        205|          110|12000.0|             206|
                                    |        206|          110| 8300.0|            null|
                                    |        201|           20|13000.0|             202|
                                    |        202|           20| 6000.0|            null|
                                    |        114|           30|11000.0|             115|
                                    |        115|           30| 3100.0|             116|
                                    |        116|           30| 2900.0|             117|
                                    |        117|           30| 2800.0|             118|
                                    |        118|           30| 2600.0|             119|
                                    |        119|           30| 2500.0|            null|
                                    |        203|           40| 6500.0|            null|
                                    |        121|           50| 8200.0|             120|
                                    |        120|           50| 8000.0|             122|
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
                                    |        200|           10| 4400.0|            null|       null|
                                    |        108|          100|12000.0|             109|     9000.0|
                                    |        109|          100| 9000.0|             110|     8200.0|
                                    |        110|          100| 8200.0|             112|     7800.0|
                                    |        112|          100| 7800.0|             111|     7700.0|
                                    |        111|          100| 7700.0|             113|     6900.0|
                                    |        113|          100| 6900.0|            null|       null|
                                    |        205|          110|12000.0|             206|     8300.0|
                                    |        206|          110| 8300.0|            null|       null|
                                    |        201|           20|13000.0|             202|     6000.0|
                                    |        202|           20| 6000.0|            null|       null|
                                    |        114|           30|11000.0|             115|     3100.0|
                                    |        115|           30| 3100.0|             116|     2900.0|
                                    |        116|           30| 2900.0|             117|     2800.0|
                                    |        117|           30| 2800.0|             118|     2600.0|
                                    |        118|           30| 2600.0|             119|     2500.0|
                                    |        119|           30| 2500.0|            null|       null|
                                    |        203|           40| 6500.0|            null|       null|
                                    |        121|           50| 8200.0|             120|     8000.0|
                                    |        120|           50| 8000.0|             122|     7900.0|
                                    +-----------+-------------+-------+----------------+-----------+
                                    only showing top 20 rows

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
                                    |        200|           10| 4400.0|            null|       null|
                                    |        108|          100|12000.0|             109|     3000.0|
                                    |        109|          100| 9000.0|             110|      800.0|
                                    |        110|          100| 8200.0|             112|      400.0|
                                    |        112|          100| 7800.0|             111|      100.0|
                                    |        111|          100| 7700.0|             113|      800.0|
                                    |        113|          100| 6900.0|            null|       null|
                                    |        205|          110|12000.0|             206|     3700.0|
                                    |        206|          110| 8300.0|            null|       null|
                                    |        201|           20|13000.0|             202|     7000.0|
                                    |        202|           20| 6000.0|            null|       null|
                                    |        114|           30|11000.0|             115|     7900.0|
                                    |        115|           30| 3100.0|             116|      200.0|
                                    |        116|           30| 2900.0|             117|      100.0|
                                    |        117|           30| 2800.0|             118|      200.0|
                                    |        118|           30| 2600.0|             119|      100.0|
                                    |        119|           30| 2500.0|            null|       null|
                                    |        203|           40| 6500.0|            null|       null|
                                    |        121|           50| 8200.0|             120|      200.0|
                                    |        120|           50| 8000.0|             122|      100.0|
                                    +-----------+-------------+-------+----------------+-----------+
                                    only showing top 20 rows


```
We can pass additional options to the lead function:
  + lead(salary,1): we include next item details
  + lead(salary,2): we include x2 next item details
```python

                                        >>> employees.select ('employee_id', 'department_id','salary').\
                                        ... withColumn('next_employee_id', lead('employee_id',2).over(spec)).\
                                        ... withColumn('next_salary', lead('salary',2).over(spec)).\
                                        ... sort('department_id', employees.salary.desc()).\
                                        ... show()

                                        +-----------+-------------+-------+----------------+-----------+
                                        |employee_id|department_id| salary|next_employee_id|next_salary|
                                        +-----------+-------------+-------+----------------+-----------+
                                        |        200|           10| 4400.0|            null|       null|
                                        |        108|          100|12000.0|             110|     8200.0|
                                        |        109|          100| 9000.0|             112|     7800.0|
                                        |        110|          100| 8200.0|             111|     7700.0|
                                        |        112|          100| 7800.0|             113|     6900.0|
                                        |        111|          100| 7700.0|            null|       null|
                                        |        113|          100| 6900.0|            null|       null|
                                        |        205|          110|12000.0|            null|       null|
                                        |        206|          110| 8300.0|            null|       null|
                                        |        201|           20|13000.0|            null|       null|
                                        |        202|           20| 6000.0|            null|       null|
                                        |        114|           30|11000.0|             116|     2900.0|
                                        |        115|           30| 3100.0|             117|     2800.0|
                                        |        116|           30| 2900.0|             118|     2600.0|
                                        |        117|           30| 2800.0|             119|     2500.0|
                                        |        118|           30| 2600.0|            null|       null|
                                        |        119|           30| 2500.0|            null|       null|
                                        |        203|           40| 6500.0|            null|       null|
                                        |        121|           50| 8200.0|             122|     7900.0|
                                        |        120|           50| 8000.0|             123|     6500.0|
                                        +-----------+-------------+-------+----------------+-----------+
                                        only showing top 20 rows
```

Here, we are sayin in the column "next_employee_id" the second best paid salary and, in next_salary column,the amount that corresponds to that employee.
                                        +-----------+-------------+-------+----------------+-----------+
                                        |employee_id|department_id| salary|next_employee_id|next_salary|
                                        +-----------+-------------+-------+----------------+-----------+
                                        |        108|          100|12000.0|             110|     8200.0|


** Exercise **: get previous employee detail.
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
                                        |        200| 4400.0|           10|      null|
                                        |        108|12000.0|          100|      null|
                                        |        109| 9000.0|          100|   12000.0|
                                        |        110| 8200.0|          100|    9000.0|
                                        |        112| 7800.0|          100|    8200.0|
                                        |        111| 7700.0|          100|    7800.0|
                                        |        113| 6900.0|          100|    7700.0|
                                        |        205|12000.0|          110|      null|
                                        |        206| 8300.0|          110|   12000.0|
                                        |        201|13000.0|           20|      null|
                                        |        202| 6000.0|           20|   13000.0|
                                        |        114|11000.0|           30|      null|
                                        |        115| 3100.0|           30|   11000.0|
                                        |        116| 2900.0|           30|    3100.0|
                                        |        117| 2800.0|           30|    2900.0|

```

Here we show previous employee_id as well as his salary:
```python

                                        >>> employeesLag = employees. \
                                        ...   select('employee_id', 'salary', 'department_id'). \
                                        ...   withColumn('lag_employee_id', lag(employees.employee_id, 1).over(spec)). \
                                        ...   withColumn('lag_salary', lag(employees.salary, 1).over(spec)). \
                                        ...   orderBy(employees.department_id, employees.salary.desc())
                                        >>> employeesLag.show(200)
                                        +-----------+-------+-------------+---------------+----------+
                                        |employee_id| salary|department_id|lag_employee_id|lag_salary|
                                        +-----------+-------+-------------+---------------+----------+
                                        |        200| 4400.0|           10|           null|      null|
                                        |        108|12000.0|          100|           null|      null|
                                        |        109| 9000.0|          100|            108|   12000.0|
                                        |        110| 8200.0|          100|            109|    9000.0|
                                        |        112| 7800.0|          100|            110|    8200.0|
                                        |        111| 7700.0|          100|            112|    7800.0|
                                        |        113| 6900.0|          100|            111|    7700.0|
                                        |        205|12000.0|          110|           null|      null|
                                        |        206| 8300.0|          110|            205|   12000.0|


```
Example with the first function: It gets the best paid salary and the employee_id it belongs to within each department.
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
                                        |        200| 4400.0|           10|      4400.0|
                                        |        108|12000.0|          100|     12000.0|
                                        |        109| 9000.0|          100|     12000.0|
                                        |        110| 8200.0|          100|     12000.0|
                                        |        112| 7800.0|          100|     12000.0|
                                        |        111| 7700.0|          100|     12000.0|
                                        |        113| 6900.0|          100|     12000.0|
                                        |        205|12000.0|          110|     12000.0|
                                        |        206| 8300.0|          110|     12000.0|
                                        |        201|13000.0|           20|     13000.0|
                                        |        202| 6000.0|           20|     13000.0|
                                        |        114|11000.0|           30|     11000.0|
                                        |        115| 3100.0|           30|     11000.0|
                                        |        116| 2900.0|           30|     11000.0|
                                        |        117| 2800.0|           30|     11000.0|
                                        |        118| 2600.0|           30|     11000.0|
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
                                        |        200| 4400.0|           10|      4400.0|
                                        |        108|12000.0|          100|     12000.0|
                                        |        109| 9000.0|          100|      9000.0|
                                        |        110| 8200.0|          100|      8200.0|
                                        |        112| 7800.0|          100|      7800.0|
                                        |        111| 7700.0|          100|      7700.0|
                                        |        113| 6900.0|          100|      6900.0|
                                        |        205|12000.0|          110|     12000.0|
                                        |        206| 8300.0|          110|      8300.0|
                                        |        201|13000.0|           20|     13000.0|
                                        |        202| 6000.0|           20|      6000.0|
                                        |        114|11000.0|           30|     11000.0|
                                        |        115| 3100.0|           30|      3100.0|
                                        |        116| 2900.0|           30|      2900.0|
                                        |        117| 2800.0|           30|      2800.0|
                                        |        118| 2600.0|           30|      2600.0|
                                        |        119| 2500.0|           30|      2500.0|
                                        |        203| 6500.0|           40|      6500.0|
                                        |        121| 8200.0|           50|      8200.0|
                                        |        120| 8000.0|           50|      8000.0|
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
                                        |        200| 4400.0|           10|     4400.0|
                                        |        108|12000.0|          100|     6900.0|
                                        |        109| 9000.0|          100|     6900.0|
                                        |        110| 8200.0|          100|     6900.0|
                                        |        112| 7800.0|          100|     6900.0|
                                        |        111| 7700.0|          100|     6900.0|
                                        |        113| 6900.0|          100|     6900.0|
                                        |        205|12000.0|          110|     8300.0|
                                        |        206| 8300.0|          110|     8300.0|
                                        |        201|13000.0|           20|     6000.0|
                                        |        202| 6000.0|           20|     6000.0|
                                        |        114|11000.0|           30|     2500.0|
                                        |        115| 3100.0|           30|     2500.0|
                                        |        116| 2900.0|           30|     2500.0|
                                        |        117| 2800.0|           30|     2500.0|
                                        |        118| 2600.0|           30|     2500.0|
                                        |        119| 2500.0|           30|     2500.0|

```



### Ranking functions - rank,dense_rank,row_number, etc

The data normally is partitioned by a key (such as department) and then sorted by some other key (such us salary).

+ We have functions like rank, dense_rank, row_number, etc
+ We need to create a WindowSpec object using partitionBy and then orderBy for most of the ranking functions.
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
                                |        200|           10| 4400.0|
                                |        108|          100|12000.0|
                                |        109|          100| 9000.0|
                                |        110|          100| 8200.0|
                                |        112|          100| 7800.0|
                                |        111|          100| 7700.0|
                                |        113|          100| 6900.0|
                                |        205|          110|12000.0|
                                |        206|          110| 8300.0|
                                |        201|           20|13000.0|
                                |        202|           20| 6000.0|
                                |        114|           30|11000.0|
                                |        115|           30| 3100.0|
                                |        116|           30| 2900.0|
                                |        117|           30| 2800.0|
                                |        118|           30| 2600.0|
                                |        119|           30| 2500.0|
                                |        203|           40| 6500.0|
                                |        121|           50| 8200.0|
                                |        120|           50| 8000.0|
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
                                |        200|           10| 4400.0|   1|
                                |        108|          100|12000.0|   1|
                                |        109|          100| 9000.0|   2|
                                |        110|          100| 8200.0|   3|
                                |        112|          100| 7800.0|   4|
                                |        111|          100| 7700.0|   5|
                                |        113|          100| 6900.0|   6|
                                |        205|          110|12000.0|   1|
                                |        206|          110| 8300.0|   2|
                                |        201|           20|13000.0|   1|
                                |        202|           20| 6000.0|   2|
                                |        114|           30|11000.0|   1|
                                |        115|           30| 3100.0|   2|
                                |        116|           30| 2900.0|   3|
                                |        117|           30| 2800.0|   4|
                                |        118|           30| 2600.0|   5|
                                |        119|           30| 2500.0|   6|
                                |        203|           40| 6500.0|   1|
                                |        121|           50| 8200.0|   1|
                                |        120|           50| 8000.0|   2|
                                +-----------+-------------+-------+----+
                                only showing top 20 rows
```

The difference between rank and dense_rank is taht, if two employees are having the same salary in a department, they will be given the same rank position with the rank function:
With dense_rank, despite the fact of having the same salary, they would be given consecutive positions.

### Dense_rank:
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
                                |        200|           10| 4400.0|   1|         1|
                                |        108|          100|12000.0|   1|         1|
                                |        109|          100| 9000.0|   2|         2|
                                |        110|          100| 8200.0|   3|         3|
                                |        112|          100| 7800.0|   4|         4|
                                |        111|          100| 7700.0|   5|         5|
                                |        113|          100| 6900.0|   6|         6|
                                |        205|          110|12000.0|   1|         1|
                                |        206|          110| 8300.0|   2|         2|
                                |        201|           20|13000.0|   1|         1|
                                |        202|           20| 6000.0|   2|         2|
                                |        114|           30|11000.0|   1|         1|
                                |        115|           30| 3100.0|   2|         2|
                                |        116|           30| 2900.0|   3|         3|
                                |        117|           30| 2800.0|   4|         4|
                                |        118|           30| 2600.0|   5|         5|
                                |        119|           30| 2500.0|   6|         6|
                                |        203|           40| 6500.0|   1|         1|
                                |        121|           50| 8200.0|   1|         1|
                                |        120|           50| 8000.0|   2|         2|
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
                                |        200|           10| 4400.0|   1|         1|         1|
                                |        108|          100|12000.0|   1|         1|         1|
                                |        109|          100| 9000.0|   2|         2|         2|
                                |        110|          100| 8200.0|   3|         3|         3|
                                |        112|          100| 7800.0|   4|         4|         4|
                                |        111|          100| 7700.0|   5|         5|         5|
                                |        113|          100| 6900.0|   6|         6|         6|
                                |        205|          110|12000.0|   1|         1|         1|
                                |        206|          110| 8300.0|   2|         2|         2|
                                |        201|           20|13000.0|   1|         1|         1|
                                |        202|           20| 6000.0|   2|         2|         2|
                                |        114|           30|11000.0|   1|         1|         1|
                                |        115|           30| 3100.0|   2|         2|         2|
                                |        116|           30| 2900.0|   3|         3|         3|
                                |        117|           30| 2800.0|   4|         4|         4|
                                |        118|           30| 2600.0|   5|         5|         5|
                                |        119|           30| 2500.0|   6|         6|         6|
                                |        203|           40| 6500.0|   1|         1|         1|
                                |        121|           50| 8200.0|   1|         1|         1|
                                |        120|           50| 8000.0|   2|         2|         2|
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
                                |        200|           10| 4400.0|   1|         1|         1|
                                |        108|          100|12000.0|   1|         1|         1|
                                |        109|          100| 9000.0|   2|         2|         2|
                                |        110|          100| 8200.0|   3|         3|         3|
                                |        112|          100| 7800.0|   4|         4|         4|
                                |        111|          100| 7700.0|   5|         5|         5|
                                |        113|          100| 6900.0|   6|         6|         6|
                                |        205|          110|12000.0|   1|         1|         1|
                                |        206|          110| 8300.0|   2|         2|         2|
                                |        201|           20|13000.0|   1|         1|         1|
                                |        202|           20| 6000.0|   2|         2|         2|
                                |        114|           30|11000.0|   1|         1|         1|
                                |        115|           30| 3100.0|   2|         2|         2|
                                |        116|           30| 2900.0|   3|         3|         3|
                                |        117|           30| 2800.0|   4|         4|         4|
                                |        118|           30| 2600.0|   5|         5|         5|
                                |        119|           30| 2500.0|   6|         6|         6|
                                |        203|           40| 6500.0|   1|         1|         1|
                                |        121|           50| 8200.0|   1|         1|         1|
                                |        120|           50| 8000.0|   2|         2|         2|
                                +-----------+-------------+-------+----+----------+----------+
                                only showing top 20 rows

```
- Some realistic use cases:
  + Assign rank to employees based on salary within each department.
  + Assign ranks to products based on revenue each day or month.

To know how to manage the resources, we have to know the size of the file we will be working with, as well as the capicity of the cluster:
```python

    [carlos_sanchez@gw03 ~]$ hdfs dfs -ls -h /public/crime/csv
    Found 1 items
    -rw-r--r--   3 hdfs hdfs      1.4 G 2017-08-08 04:34 /public/crime/csv/crime_data.csv

```

As part of the certification exam, you will be given the resource manager page or you gill be given the resource manager name (the port for the resource manager is always 8088):
rm01.itversity.com:19088



### Cluster Metrics
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





    **Problem**- Get monthly crime count by type

Steps:

            # Launch pyspark with optimal resources
 

            # Read data from HDFS into RDD


            # Convert each element into tuple (month,crime_type),1)


            # Perform aggregation to get ((month, crime_type))


            # Convert each element into ((month, -count), month + "\t" + count + "\t" + crime_type)


            # Sort the data


            # Discar the key and get the value - month + "\t" + count + "\t" + crime_type


            #Sava into HDFS using compression


            # make sure number of files are either one or two

