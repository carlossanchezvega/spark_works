RDDs EXERCISED FOR CCA175 CLOUDERA PYSPARK DEVELOPER
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


##EXERCISES USING RDDs

**Exercise 1**
Get total revenue for items with key = 2.

Order revenue is provided in table orderItems

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

Then, we need to filter orders with key ==2
```python
>>> orderItemsFiltered = orderItems.filter(lambda oi: int(oi.split(",")[1])==2)
>>> for i in orderItemsFiltered.take(10):print(i)
... 
2,2,1073,1,199.99,199.99
3,2,502,5,250.0,50.0
4,2,403,1,129.99,129.99
```

Now, from those rows, we need to take the total revenue:
Then. we try to get the field corresponding to the revenue

```python
>>> orderItemsSubtotal = orderItemsFiltered.map(lambda oi:float(oi.split(",")[4]))
>>> for i in orderItemsSubtotal.take(10):print(i)
... 
199.99
250.0
129.99
```
Now, we have to sum all the subtotals from those rows:
```python
>>> orderItemsSubtotal.reduce(lambda x,y: x+y)
579.98
```


**Exercise 2**
Get order_item details which have minimum order_item_revenue for a given order_id = 1

It means:
```python
orderItems = sc.textFile("/public/retail_db/order_items")

1,1,957,1,299.98,299.98
2,2,1073,1,199.99,199.99
3,2,502,5,250.0,50.0
4,2,403,1,129.99,129.99 ---> the one having the least revenue subtotal is this one
```

The field corresponding to the order_id in each row is the second. For that reason, we must start filtering those rows:

```python
>>> orderItemsFiltered = orderItems.filter(lambda oi: int(oi.split(",")[1])==2)
>>> for i in orderItemsFiltered.take(10):print(i)
... 
2,2,1073,1,199.99,199.99
3,2,502,5,250.0,50.0
4,2,403,1,129.99,129.99
```

Then, we have to compare the values corresponding to the revenue subtotal as for getting the one having the minimum value:

```python
>>> orderItemsFiltered.reduce(lambda x,y: x if float(x.split(",")[4])< float(y.split(",")[4]) else y)
u'4,2,403,1,129.99,129.99'
```

**Exercise 3**
Get the number of elements by status
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
```

Now, we have to filter the fields corresponding to the status (4 th element, array[3]). 
Then, we have to create tuples assigning a "1" for each of the elements in the array

```python
>>> ordersStatus = orders.map(lambda o: (o.split(",")[3],1))
>>> for i in ordersStatus.take(10):print(i)
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
```

Now, having all those tuples, we will consider the status as the key so we can use the countByKey function to get the total number

```python
>>> ordersStatus.countByKey()
defaultdict(<type 'int'>, {u'COMPLETE': 22899, u'PAYMENT_REVIEW': 729, u'PROCESSING': 8275, u'CANCELED': 1428, u'PENDING': 7610, u'CLOSED': 7556, u'PENDING_PAYMENT': 15030, u'SUSPECTED_FRAUD': 1558, u'ON_HOLD': 3798})

```

**Important**---> countByKey

**Exercise 4**
Get revenue for each order_id:
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

Firstly, we have to create some tuples filtering, for each row, (order_id, revenue):

```python
orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]),float(oi.split(",")[4])))
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

Now, we have to group the elements having the same order_id. For that purpose, we use the "groupByKey" function
```python
>>> orderItemsPerOrderId = orderItemsMap.groupByKey()
>>> for i in orderItemsPerOrderId.take(10):print(i)
... 
(2, <pyspark.resultiterable.ResultIterable object at 0x7f60c6eb5910>)           
(4, <pyspark.resultiterable.ResultIterable object at 0x7f60c6eb5890>)
(8, <pyspark.resultiterable.ResultIterable object at 0x7f60c6eb5990>)
(10, <pyspark.resultiterable.ResultIterable object at 0x7f60c6eb59d0>)
(12, <pyspark.resultiterable.ResultIterable object at 0x7f60c6eb5c90>)
(14, <pyspark.resultiterable.ResultIterable object at 0x7f60c6eb5810>)
(16, <pyspark.resultiterable.ResultIterable object at 0x7f60c6eb5a90>)
(18, <pyspark.resultiterable.ResultIterable object at 0x7f60c6eb5750>)
(20, <pyspark.resultiterable.ResultIterable object at 0x7f60c6eb5190>)
(24, <pyspark.resultiterable.ResultIterable object at 0x7f60c6eb5a10>)
```
As we can see, when grouping what we do is to create data structures and, 
for each key, it contains an array with all the revenues for that jey:

```python
>>> orderItemsPerOrderId.first()
(2, <pyspark.resultiterable.ResultIterable object at 0x7f60c6eb55d0>)
>>> orderItemsPerOrderId.first()[1]
<pyspark.resultiterable.ResultIterable object at 0x7f60c6eaae90>
>>> list(orderItemsPerOrderId.first()[1])
[199.99, 250.0, 129.99]
```
so what we need is to execute the sum of the elements corresponding to each key:

```python
>>> revenuePerOrderId = orderItemsPerOrderId.map(lambda oi: (int(oi[0]),sum(oi[1]))
>>> for i in revenuePerOrderId.take(10):print(i)
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

**Important**---> groupByKey
**Exercise 5**
Get orderItems details in descending order by revenue

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
Firstly, we have to group elements by orderId:

```python
orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]),oi))
for i in orderItemsMap.take(10):print(i)
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

```
Now, we have to group the orders having the same orderId (the same key):
```python
>>> orderItemsGroupByKey = orderItemsMap.groupByKey()
>>> for i in orderItemsGroupByKey.take(10):print(i)
... 
(2, <pyspark.resultiterable.ResultIterable object at 0x7f60c6ea7f10>)           
(4, <pyspark.resultiterable.ResultIterable object at 0x7f60c71aae50>)
(8, <pyspark.resultiterable.ResultIterable object at 0x7f60c71aadd0>)
(10, <pyspark.resultiterable.ResultIterable object at 0x7f60c71aa750>)
(12, <pyspark.resultiterable.ResultIterable object at 0x7f60c71aa950>)
(14, <pyspark.resultiterable.ResultIterable object at 0x7f60c71aa7d0>)
(16, <pyspark.resultiterable.ResultIterable object at 0x7f60c71aabd0>)
(18, <pyspark.resultiterable.ResultIterable object at 0x7f60c71aac90>)
(20, <pyspark.resultiterable.ResultIterable object at 0x7f60c71aafd0>)
(24, <pyspark.resultiterable.ResultIterable object at 0x7f60c71aaed0>)
```
As we can see, the second element of each tuple is the grouping of all the elements having the same orderId.

Then, we have to order those elements. For that purpose we will use the "sorted" function
```python
>>> orderItemsSortedBySubtotalPerOrder = orderItemsGroupByKey.map(lambda oi: sorted(oi[1],key = lambda k: float(k.split(",")[4]),reverse = True ))
>>> help(sorted)

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
>>> 
```

As we can see, elements are sorted per key. If we wanted to print one element per lne, we should have to use the flatMap function in order to flatten each row and get a new line:

```python
>>> orderItemsSortedBySubtotalPerOrder = orderItemsGroupByKey.flatMap(lambda oi: sorted(oi[1],key = lambda k: float(k.split(",")[4]),reverse = True ))
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
>>> 

```
**Important**---> sorted and flatMap


**Exercise 5**
Get min revenue per each orderId

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

>>> minSubtotalPerOrderId = orderItemsMap.reduceByKey(lambda x,y: x if (x<y) else y )
>>> 
>>> for i in minSubtotalPerOrderId.take(10):print(i)
... 

(2, u'2,2,1073,1,199.99,199.99')                                                
(4, u'5,4,897,2,49.98,24.99')
(8, u'17,8,365,3,179.97,59.99')
(10, u'24,10,1073,1,199.99,199.99')
(12, u'34,12,957,1,299.98,299.98')
(14, u'40,14,1004,1,399.98,399.98')
(16, u'48,16,365,2,119.98,59.99')
(18, u'55,18,1073,1,199.99,199.99')
(20, u'60,20,502,5,250.0,50.0')
(24, u'69,24,403,1,129.99,129.99')

```
**Important**---> reduceByKey


**Exercise 5**
Get order item details with minimum subtotal for each order_id

```python
orderItems = sc.textFile("/public/retail_db/order_items")
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

#Now, we have to compare the elements
>>> minSubtotalPerOrderId = orderItemsMap.reduceByKey(lambda x,y:
...     x if (float(x.split(",")[4])< float(y.split(",")[4])) else y)

>>> for i in minSubtotalPerOrderId.take(10):print(i)
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

**Exercise 6**
 Get revenue and count of items for each order_id

```python
orderItems = sc.textFile("/public/retail_db/order_items")
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
Now, for each of the elements in the tuple, we have to extract the order_id and the revenue

```python
>>> orderItemsMap = orderItems.map(lambda oi:(int(oi.split(",")[1]), float(oi.split(",")[4])))
>>> for i in orderItemsMap.take(10): print(i)
... 

(1, 299.98                                                                     
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

Here, we have tuples like (orderId, orderRevenue). But, what we want to have is: 
Example:
(2,(579.98,3)) ---> we have an element of type (int, tuple).
What we want is to have:

    *Initialitation step -> (2,(0.0,0)) ---> Before executing any sum, the total revenue and count of elements is equal to 0
    * First step -> (2,199.99) + (2,250) -> (2,(449.99,2))
    * Second step ->(2,(449.99,2)) + (2,129.99) -> (2,(579,98,3))


---------------- IMPORTANTE ---------------

```python
>>> revenuePerOrder =  orderItemsMap. \
... aggregateByKey((0.0, 0),
...     lambda x,y: (x[0] + y, x[1] +1),
...     lambda x,y: (x[0] + y[0], x[1] + y[1]))

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


**Exercise 6**
Sort data by product price - sortByKey

The table which contains the information related with the products and prices is the table product

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
```

As there might be some products with NULL values, we will filter those rows:

```python
>>> productsMap = products.filter(lambda p: p.split(",")[4]!="").\
... map(lambda p: (float(p.split(",")[4]),p))
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
>>> 
```

Now, we will have to sort the data by price, which in this case is the product price

```python
productsSortedByPrice = productsMap.sortByKey()
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
As we don't need the part containning the key since we already have the data sorted, we use the map function to select the part of the tuple we want:

```python
>>> productsSortedMap = productsSortedByPrice.map(lambda p:p[1])
>>> for i in productsSortedMap.take(10):print(i)
... 
38,3,Nike Men's Hypervenom Phantom Premium FG Socc,,0.0,http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat
388,18,Nike Men's Hypervenom Phantom Premium FG Socc,,0.0,http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat
414,19,Nike Men's Hypervenom Phantom Premium FG Socc,,0.0,http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat
517,24,Nike Men's Hypervenom Phantom Premium FG Socc,,0.0,http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat
547,25,Nike Men's Hypervenom Phantom Premium FG Socc,,0.0,http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat
934,42,Callaway X Hot Driver,,0.0,http://images.acmesports.sports/Callaway+X+Hot+Driver
1284,57,Nike Men's Hypervenom Phantom Premium FG Socc,,0.0,http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat
624,29,adidas Batting Helmet Hardware Kit,,4.99,http://images.acmesports.sports/adidas+Batting+Helmet+Hardware+Kit
815,37,Zero Friction Practice Golf Balls - 12 Pack,,4.99,http://images.acmesports.sports/Zero+Friction+Practice+Golf+Balls+-+12+Pack
336,15,"Nike Swoosh Headband - 2""",,5.0,http://images.acmesports.sports/Nike+Swoosh+Headband+-+2%22
>>> 
```

**Exercise 7**
 Sort data by product category and then product price descending - sortByKey
```python
products = sc.textFile("/public/retail_db/products")
 for i in products.take(10):print(i)
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
```


Now, taking into account this tuple:
```python
1,2,Quest Q64 10 FT. x 10 FT. Slant Leg Instant U,,59.98,http://images.acmesports.sports/Quest+Q64+10+FT.+x+10+FT.+Slant+Leg+Instant+Up+Canopy
```
We have to sort the data by categoryId(which, in this case is the field with categoryId = 2) and then, by productPrice (59.98). To sort the data, we will use the function sorByKey. For that reason, the key element in the tuple, in this case, will be the elements taken into account to order the data, so we have to create a composite key (categoryId,productPrice)

```python
>>> productsMap = products.\
... filter(lambda p: p.split(",")[4]!="").\
... map(lambda p: ((int(p.split(",")[1]),- float(p.split(",")[4])),p))
>>> for i in productsMap.take(10): print(i)
... 
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
>>> 
```
Then, we sort the data

```python
>>> productsMapSorted = productsMap.sortByKey()
>>> for i in productsMapSorted.take(10):print(i)
... 
((2, -299.99), u'16,2,Riddell Youth 360 Custom Football Helmet,,299.99,http://images.acmesports.sports/Riddell+Youth+360+Custom+Football+Helmet')
((2, -209.99), u'11,2,Fitness Gear 300 lb Olympic Weight Set,,209.99,http://images.acmesports.sports/Fitness+Gear+300+lb+Olympic+Weight+Set')
((2, -199.99), u'5,2,Riddell Youth Revolution Speed Custom Footbal,,199.99,http://images.acmesports.sports/Riddell+Youth+Revolution+Speed+Custom+Football+Helmet')
((2, -199.99), u'14,2,Quik Shade Summit SX170 10 FT. x 10 FT. Canop,,199.99,http://images.acmesports.sports/Quik+Shade+Summit+SX170+10+FT.+x+10+FT.+Canopy')
((2, -139.99), u"12,2,Under Armour Men's Highlight MC Alter Ego Fla,,139.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Alter+Ego+Flash+Football...")
((2, -139.99), u"23,2,Under Armour Men's Highlight MC Alter Ego Hul,,139.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Alter+Ego+Hulk+Football...")
((2, -134.99), u"6,2,Jordan Men's VI Retro TD Football Cleat,,134.99,http://images.acmesports.sports/Jordan+Men%27s+VI+Retro+TD+Football+Cleat")
((2, -129.99), u"2,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat")
((2, -129.99), u"8,2,Nike Men's Vapor Carbon Elite TD Football Cle,,129.99,http://images.acmesports.sports/Nike+Men%27s+Vapor+Carbon+Elite+TD+Football+Cleat")
((2, -129.99), u"10,2,Under Armour Men's Highlight MC Football Clea,,129.99,http://images.acmesports.sports/Under+Armour+Men%27s+Highlight+MC+Football+Cleat")
>>> 
```

As seen above, we have added a minus sign "-" to order the data in descending order, in this case order the data in descending order by price.

Now, as we don't want the part corresponding to the tuple, but the part corresponding to the info, we have to use a map function:

```python
>>> productsMap = productsMapSorted.map(lambda p: p[1])
>>> for i in productsMap.take(10):print(i)
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
```


**Exercise 8**

Ranking: Get top N products by price- Global ranking-

What we should do is:
    1.sortByKey all the elements
    2.take the first n elements

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
1. Transform the data in tuples whose keys want to be sorted
2. sortByKey
3. map(to discard the part of the key)
4. take the first n elements:

Then, we proceed with the steps mentioned:

1.Transform the data in tuples whose keys want to be sorted
```python
>>> productsMap = products.filter(lambda p:p.split(",")[4]!="").\
... map(lambda p: (float(p.split(",")[4]),p))
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
Now, we sort the data:
```python
>>> productsSorted = productsMap.sortByKey()
>>> for i in productsSorted.take(10):print(i)
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
>>> 
```

Now, we eliminate the part  of the key:
```python
>>> productsSortedMap = productsSorted.map(lambda p: p[1]).take(5)
>>> for i in productsSortedMap:print(i)
... 
38,3,Nike Men's Hypervenom Phantom Premium FG Socc,,0.0,http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat
388,18,Nike Men's Hypervenom Phantom Premium FG Socc,,0.0,http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat
414,19,Nike Men's Hypervenom Phantom Premium FG Socc,,0.0,http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat
517,24,Nike Men's Hypervenom Phantom Premium FG Socc,,0.0,http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat
547,25,Nike Men's Hypervenom Phantom Premium FG Socc,,0.0,http://images.acmesports.sports/Nike+Men%27s+Hypervenom+Phantom+Premium+FG+Soccer+Cleat
>>> 
```

We could have done the same with the **top** function:

```python
>>> products = sc.textFile("/public/retail_db/products")
productsFiltered = products.filter(lambda p: p.split(",")[4]!="")
topNProducts = productsFiltered.top(5,key = lambda k: float(k.split(",")[4]))
>>> for i in topNProducts:print(i)
... 
208,10,SOLE E35 Elliptical,,1999.99,http://images.acmesports.sports/SOLE+E35+Elliptical
66,4,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill
199,10,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill
496,22,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill
1048,47,"Spalding Beast 60"" Glass Portable Basketball ",,1099.99,http://images.acmesports.sports/Spalding+Beast+60%22+Glass+Portable+Basketball+Hoop
```


In addition, we can solve the problem with the **takeOrdered** function

```python
>>> topNProducts = productsFiltered.takeOrdered(5,key= lambda k: -float(k.split(",")[4]))
>>> for i in topNProducts:print(i)
... 
208,10,SOLE E35 Elliptical,,1999.99,http://images.acmesports.sports/SOLE+E35+Elliptical
66,4,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill
199,10,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill
496,22,SOLE F85 Treadmill,,1799.99,http://images.acmesports.sports/SOLE+F85+Treadmill
1048,47,"Spalding Beast 60"" Glass Portable Basketball ",,1099.99,http://images.acmesports.sports/Spalding+Beast+60%22+Glass+Portable+Basketball+Hoop

```

RANKINGBY CATEGORY (NO ME ENTERO)

** SAVE FILES
** COMPRESS FILES 


**Exercise 10**
- Use retail_db data set
- Problem Statement
    * Get daily revenue by product considering completed and closed orders (include product name)
    * Data need to be sorted in ascending order by date and then descending order by revenue computed for each product for each day.
- Data for orders and order_items is available in HDFS /public/retail_db/orders and /public/retail_db/order_items
- Data for products is available locally under /data/retail_db/products
- Final output need to be stored under
    * HDFS location – avro format /user/YOUR_USER_ID/daily_revenue_avro_python
    * HDFS location – text format /user/YOUR_USER_ID/daily_revenue_txt_python
    * Local location /home/YOUR_USER_ID/daily_revenue_python
    * Solution need to be stored under /home/YOUR_USER_ID/daily_revenue_python.txt


Solution:
Firstly, we will concentrate on the part of "Get daily revenue by product considering completed and closed orders"


1. we read data:
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

Taking into account that dataset, we have to filter "COMPLETED" or "CLOSED" orders.

```python
>>> ordersFiltered = orders.filter(lambda o: o.split(",")[3] in ["COMPLETED", "CLOSED" ])
>>> for i in ordersFiltered.take(10):print(i)
1,2013-07-25 00:00:00.0,11599,CLOSED
4,2013-07-25 00:00:00.0,8827,CLOSED
12,2013-07-25 00:00:00.0,1837,CLOSED
18,2013-07-25 00:00:00.0,1205,CLOSED
24,2013-07-25 00:00:00.0,11441,CLOSED
25,2013-07-25 00:00:00.0,9503,CLOSED
37,2013-07-25 00:00:00.0,5863,CLOSED
51,2013-07-25 00:00:00.0,12271,CLOSED
57,2013-07-25 00:00:00.0,7073,CLOSED
61,2013-07-25 00:00:00.0,4791,CLOSED
```

As we are computing some analysis regarding with dates, now that we have filtered, we have to create tuple selecting date and order_id (order_id will be useful for next steps, to join with other tables whose data we will need):

```python
>>> ordersMap = ordersFiltered.map(lambda o: (int(o.split(",")[0]),o.split(",")[1]))
>>> 
>>> for i in ordersMap.take(10):print(i)
... 
(1, u'2013-07-25 00:00:00.0')
(4, u'2013-07-25 00:00:00.0')
(12, u'2013-07-25 00:00:00.0')
(18, u'2013-07-25 00:00:00.0')
(24, u'2013-07-25 00:00:00.0')
(25, u'2013-07-25 00:00:00.0')
(37, u'2013-07-25 00:00:00.0')
(51, u'2013-07-25 00:00:00.0')
(57, u'2013-07-25 00:00:00.0')
(61, u'2013-07-25 00:00:00.0')

```


Now, to get daily revenue for each product, we have to look into order_items table:

```python
>>> orderItems = sc.textFile("/public/retail_db/order_items")
>>> for i in orderItems.take(10):print(i)
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
Now, if we have this row:
```python
1,1,957,1,299.98,299.98
```
The field order_item_product_id is the 3rd(2nd position in the array of values as index starts from 0). It means, in this case, 957:
As for the total revenue, it is the fifth field (4th in the index array considering that index starts from 0). In the case of the row above, the field is the one corresponding to the order_item_subtotal.
In addition, we have to get the field corresponding to order_id in the order_items table, which is the 2nf field (1st element in the array of indexes)
```python
>>> orderItemsMap = orderItems.map(lambda oi:(int(oi.split(",")[1]),( (int(oi.split(",")[2]),float(oi.split(",")[4])))))
(u'1', (957, 299.98))
(u'2', (1073, 199.99))
(u'2', (502, 250.0))
(u'2', (403, 129.99))
(u'4', (897, 49.98))
(u'4', (365, 299.95))
(u'4', (502, 150.0))
(u'4', (1014, 199.92))
(u'5', (957, 299.98))
(u'5', (365, 299.95))
```


So, to sum up. Considering this row:
```python
(u'1', (957, 299.98))
1->  order_item_order_id (for joining with order dataset)
957-> productId (for joining with product table) 
299.98->order_item_subtotal (total revenue)
```


Now, we will be able to join ordersMap with orderItemsMap (as we have "columns" relating both RDDs):

```python
>>> ordersJoin = ordersMap.join(orderItemsMap)
>>> for i in ordersJoin.take(10):print(i)
... 
(4, (u'2013-07-25 00:00:00.0', (897, 49.98)))                                  
(4, (u'2013-07-25 00:00:00.0', (365, 299.95)))
(4, (u'2013-07-25 00:00:00.0', (502, 150.0)))
(4, (u'2013-07-25 00:00:00.0', (1014, 199.92)))
(12, (u'2013-07-25 00:00:00.0', (957, 299.98)))
(12, (u'2013-07-25 00:00:00.0', (134, 100.0)))
(12, (u'2013-07-25 00:00:00.0', (1014, 149.94)))
(12, (u'2013-07-25 00:00:00.0', (191, 499.95)))
(12, (u'2013-07-25 00:00:00.0', (502, 250.0)))
(38232, (u'2014-03-17 00:00:00.0', (957, 299.98)))
```
Now, we can perform an aggregation operation in that RDD (aggregateByKey or reduceByKey)

Having this row
```python
(4, (u'2013-07-25 00:00:00.0', (897, 49.98)))                                  

```
We have to create a data structure so as to have, for each row, the date and its total revenue.
It means, we don't need the first part of the tuple, which belongs to the orderId ("4" in this case). In addition, we should have, for each row ((date, productId), totalRevenue)
Then, having this row:
```python
(u'2013-07-25 00:00:00.0', (897, 49.98) -> ((u'2013-07-25 00:00:00.0',897) 49.98)
```

We will show in this example above the fields which sould be taken from each row:
```python
>>> t = (4, (u'2013-07-25 00:00:00.0', (897, 49.98)))
>>> t[0] 
4
>>> t[1][0] 
u'2013-07-25 00:00:00.0'
>>> t[1][1][0] 
897
>>> t[1][1][1] 
49.98
```

so, each row should have the next structure:
```python
(t[1][0], (int(t[1][1][0]),float(t[1][1][1] ))
```

As for orderJoin RDD:

```python
>>> ordersJoinMap = ordersJoin.map( lambda o: ((o[1][0], int(o[1][1][0])),float(o[1][1][1] )))
>>> for i in ordersJoinMap.take(10):print(i)
... 
((u'2013-07-25 00:00:00.0', 897), 49.98)
((u'2013-07-25 00:00:00.0', 365), 299.95)
((u'2013-07-25 00:00:00.0', 502), 150.0)
((u'2013-07-25 00:00:00.0', 1014), 199.92)
((u'2013-07-25 00:00:00.0', 957), 299.98)
((u'2013-07-25 00:00:00.0', 134), 100.0)
((u'2013-07-25 00:00:00.0', 1014), 149.94)
((u'2013-07-25 00:00:00.0', 191), 499.95)
((u'2013-07-25 00:00:00.0', 502), 250.0)
((u'2014-03-17 00:00:00.0', 957), 299.98)

```

Now, we can use the reduceByKey function
```python
from operator import add
dailyRevenuePerProductId = ordersJoinMap.reduceByKey(add)
for i in dailyRevenuePerProductId.take(10):print(i)
((u'2014-04-01 00:00:00.0', 793), 14.99)
((u'2014-07-14 00:00:00.0', 1014), 449.82)
((u'2014-01-11 00:00:00.0', 957), 1499.9)
((u'2013-08-14 00:00:00.0', 572), 119.97)
((u'2013-10-31 00:00:00.0', 502), 1250.0)
((u'2013-12-27 00:00:00.0', 885), 24.99)
((u'2013-09-30 00:00:00.0', 365), 1139.81)
((u'2013-12-05 00:00:00.0', 957), 1499.9)
((u'2014-06-11 00:00:00.0', 24), 319.96)
((u'2014-01-20 00:00:00.0', 977), 89.97)
```

Now, we have to join with product RDD to get product name,
so to join both RDDs we need that productId is the key of the tuple:

Example:
```python
((u'2014-04-01 00:00:00.0', 793), 14.99) -> ((793,u'2014-04-01 00:00:00.0' ), 14.99)
```
```python
>>> dailyRevenuePerProductIdMap = dailyRevenuePerProductId.map(lambda d:(d[0][1],(d[0][0], d[1])) )
>>> for i in dailyRevenuePerProductIdMap.take(10):print(i)
... 
(793, (u'2014-04-01 00:00:00.0', 14.99))
(1014, (u'2014-07-14 00:00:00.0', 449.82))
(957, (u'2014-01-11 00:00:00.0', 1499.9))
(572, (u'2013-08-14 00:00:00.0', 119.97))
(502, (u'2013-10-31 00:00:00.0', 1250.0))
(885, (u'2013-12-27 00:00:00.0', 24.99))
(365, (u'2013-09-30 00:00:00.0', 1139.81))
(957, (u'2013-12-05 00:00:00.0', 1499.9))
(24, (u'2014-06-11 00:00:00.0', 319.96))
(977, (u'2014-01-20 00:00:00.0', 89.97))
```

As far as product RDD is concerned:
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
```
From this RDD, we have to get the part corresponding to productId(for joining with other RDDs) and productName (we need it due to the problem statement)
```python
products = sc.textFile("/public/retail_db/products")
>>> productsMap = products.map(lambda p:(int(p.split(",")[0]),p.split(",")[2] )
... )
>>> for i in productsMap.take(10):print(i)
... 
(1, u'Quest Q64 10 FT. x 10 FT. Slant Leg Instant U')
(2, u"Under Armour Men's Highlight MC Football Clea")
(3, u"Under Armour Men's Renegade D Mid Football Cl")
(4, u"Under Armour Men's Renegade D Mid Football Cl")
(5, u'Riddell Youth Revolution Speed Custom Footbal')
(6, u"Jordan Men's VI Retro TD Football Cleat")
(7, u'Schutt Youth Recruit Hybrid Custom Football H')
(8, u"Nike Men's Vapor Carbon Elite TD Football Cle")
(9, u'Nike Adult Vapor Jet 3.0 Receiver Gloves')
(10, u"Under Armour Men's Highlight MC Football Clea")
>>> 
```



Now,we can join this RDD with the "productsMap":

```python
>>> dailyRevenuePerProductIdJoin = dailyRevenuePerProductIdMap.join(productsMap)
>>> for i in dailyRevenuePerProductIdJoin.take(10):print(i)
... 
(24, ((u'2014-06-11 00:00:00.0', 319.96), u'Elevation Training Mask 2.0'))
(24, ((u'2014-01-26 00:00:00.0', 239.97), u'Elevation Training Mask 2.0'))
(24, ((u'2014-03-07 00:00:00.0', 319.96), u'Elevation Training Mask 2.0'))
(24, ((u'2013-09-01 00:00:00.0', 399.95), u'Elevation Training Mask 2.0'))
(564, ((u'2014-06-24 00:00:00.0', 120.0), u"Nike Men's Deutschland Weltmeister Winners Bl"))
(564, ((u'2013-09-22 00:00:00.0', 150.0), u"Nike Men's Deutschland Weltmeister Winners Bl"))
(564, ((u'2014-03-12 00:00:00.0', 120.0), u"Nike Men's Deutschland Weltmeister Winners Bl"))
(564, ((u'2013-10-15 00:00:00.0', 150.0), u"Nike Men's Deutschland Weltmeister Winners Bl"))
(564, ((u'2013-10-20 00:00:00.0', 30.0), u"Nike Men's Deutschland Weltmeister Winners Bl"))
(564, ((u'2013-10-23 00:00:00.0', 30.0), u"Nike Men's Deutschland Weltmeister Winners Bl"))
```
Now, from that RDD we want to eliminate the part of the productId
Say we have:
```python
(24, ((u'2014-06-11 00:00:00.0', 319.96), u'Elevation Training Mask 2.0'))
```
    * 24: productId
    * 2014-06-11 00:00:00.0: date of the order
    * Elevation Training Mask 2.0: product name

we select the fields we need:
we create the next data structure:
```python
((date, totalRevenue),date,productName)
 Considering d as the tuple above we could say:
date -> d[1][0][0]
totalRevenue -> d[1][0][1]
productName -> ,d[1][1]
and then:
((date, totalRevenue),date,totalRevenue,productName) -> (d[1][0][0], d[1][0][1] ),d[1][0][0],d[1][0][1],d[1][1])

```
As we can see, we have created the tuple (date, -totalRevenue) for ordering issues. Furthermore, we have added the minus sign "-" to order the data in descending order.

```python
>>> dailyRevenuePerProductIdJoinMap = dailyRevenuePerProductIdJoin.map(lambda d: ((d[1][0][0],- d[1][0][1] ),d[1][0][0]+","+str(d[1][0][1])+","+d[1][1]))
>>> for i in dailyRevenuePerProductIdJoinMap.take(10):print(i)
... 
((u'2014-06-11 00:00:00.0', -319.96), u'2014-06-11 00:00:00.0,319.96,Elevation Training Mask 2.0')
((u'2014-01-26 00:00:00.0', -239.97), u'2014-01-26 00:00:00.0,239.97,Elevation Training Mask 2.0')
((u'2014-03-07 00:00:00.0', -319.96), u'2014-03-07 00:00:00.0,319.96,Elevation Training Mask 2.0')
((u'2013-09-01 00:00:00.0', -399.95), u'2013-09-01 00:00:00.0,399.95,Elevation Training Mask 2.0')
((u'2014-06-24 00:00:00.0', -120.0), u"2014-06-24 00:00:00.0,120.0,Nike Men's Deutschland Weltmeister Winners Bl")
((u'2013-09-22 00:00:00.0', -150.0), u"2013-09-22 00:00:00.0,150.0,Nike Men's Deutschland Weltmeister Winners Bl")
((u'2014-03-12 00:00:00.0', -120.0), u"2014-03-12 00:00:00.0,120.0,Nike Men's Deutschland Weltmeister Winners Bl")
((u'2013-10-15 00:00:00.0', -150.0), u"2013-10-15 00:00:00.0,150.0,Nike Men's Deutschland Weltmeister Winners Bl")
((u'2013-10-20 00:00:00.0', -30.0), u"2013-10-20 00:00:00.0,30.0,Nike Men's Deutschland Weltmeister Winners Bl")
((u'2013-10-23 00:00:00.0', -30.0), u"2013-10-23 00:00:00.0,30.0,Nike Men's Deutschland Weltmeister Winners Bl")
>>> dailyRevenuePerProductIdJoinMapSorted  = dailyRevenuePerProductIdJoinMap.sortByKey()
>>> for i in  dailyRevenuePerProductIdJoinMapSorted.take(10):print(i)
... 
((u'2013-07-25 00:00:00.0', -2399.88), u'2013-07-25 00:00:00.0,2399.88,Field & Stream Sportsman 16 Gun Fire Safe')
((u'2013-07-25 00:00:00.0', -1599.92), u'2013-07-25 00:00:00.0,1599.92,Pelican Sunstream 100 Kayak')
((u'2013-07-25 00:00:00.0', -1499.9), u"2013-07-25 00:00:00.0,1499.9,Diamondback Women's Serene Classic Comfort Bi")
((u'2013-07-25 00:00:00.0', -1379.77), u'2013-07-25 00:00:00.0,1379.77,Perfect Fitness Perfect Rip Deck')
((u'2013-07-25 00:00:00.0', -1199.8799999999999), u"2013-07-25 00:00:00.0,1199.88,Nike Men's Free 5.0+ Running Shoe")
((u'2013-07-25 00:00:00.0', -900.0), u"2013-07-25 00:00:00.0,900.0,Nike Men's Dri-FIT Victory Golf Polo")
((u'2013-07-25 00:00:00.0', -649.95), u"2013-07-25 00:00:00.0,649.95,Nike Men's CJ Elite 2 TD Football Cleat")
((u'2013-07-25 00:00:00.0', -599.99), u'2013-07-25 00:00:00.0,599.99,Bowflex SelectTech 1090 Dumbbells')
((u'2013-07-25 00:00:00.0', -599.76), u"2013-07-25 00:00:00.0,599.76,O'Brien Men's Neoprene Life Vest")
((u'2013-07-25 00:00:00.0', -207.96), u'2013-07-25 00:00:00.0,207.96,Titleist Pro V1 High Numbers Personalized Gol')

```
Now, we have to eliminate the part of the key:

```python
>>> dailyRevenuePerProductName = dailyRevenuePerProductIdJoinMapSorted.map(lambda dr: (dr[1]))
>>> for i in dailyRevenuePerProductName.take(10):print(i)
... 
2013-07-25 00:00:00.0,2399.88,Field & Stream Sportsman 16 Gun Fire Safe
2013-07-25 00:00:00.0,1599.92,Pelican Sunstream 100 Kayak
2013-07-25 00:00:00.0,1499.9,Diamondback Women's Serene Classic Comfort Bi
2013-07-25 00:00:00.0,1379.77,Perfect Fitness Perfect Rip Deck
2013-07-25 00:00:00.0,1199.88,Nike Men's Free 5.0+ Running Shoe
2013-07-25 00:00:00.0,900.0,Nike Men's Dri-FIT Victory Golf Polo
2013-07-25 00:00:00.0,649.95,Nike Men's CJ Elite 2 TD Football Cleat
2013-07-25 00:00:00.0,599.99,Bowflex SelectTech 1090 Dumbbells
2013-07-25 00:00:00.0,599.76,O'Brien Men's Neoprene Life Vest
2013-07-25 00:00:00.0,207.96,Titleist Pro V1 High Numbers Personalized Gol
>>> 
```

Now, we have to save in HDFS the result, in avro format:
```python
dailyRevenuePerProductName.saveAsTextFile("/user/carlos_sanchez/daily_revenue_txt_python")
```
As we can see, the file has been saved correctly: ```python
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
(we see the file - just for txt format)
```

If we read the files:
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
```
As the problem statement mentions we have to save the data in two files, we use the coalesce function:


```python
dailyRevenuePerProductName.coalesce(2).saveAsTextFile("/user/carlos_sanchez/daily_revenue_txt_python")
```

Now, we will create a dataframe out of it and we will save it in avro format.
```python        
dailyRevenuePerProductNameDF = dailyRevenuePerProductName. \
coalesce(2). \
toDF(schema=["order_date", "revenue_per_product", "product_name"])

dailyRevenuePerProductNameDF. \
save("/user/csanchez/daily_revenue_avro_python", "com.databricks.spark.avro")
```

and the last step is to copy the files to the local file system:

For txt files
```python
hadoop fs -get /user/carlos_sanchez/daily_revenue_txt_python /home/
carlos_sanchez/daily_revenue_python/daily_revenue_txt_python
```
For avro files
```
    hadoop fs -get /user/carlos_sanchez/daily_revenue_avro_python /home/carlos_sanchez/daily_revenue_python/daily_revenue_avro_python
```

**Important**
In order to create a directory path:

```python
mkdir -p /home/carlos_sanchez/daily_revenue_python
```
**Important**
Spark supports orc and parquet formats but not avro, so we need to convert it use a plugin from databricks. If in the problem statement we are given the jar we have to use, we have to launch Spark in the next way:
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