# Databricks notebook source
# MAGIC %md
# MAGIC ##Read CSV file

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Display CSV file

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Check structure of columns 

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Change Datatype as you wish

# COMMAND ----------

my_ddl_schema = '''
                    Item_Identifier string, 
                    Item_Weight string ,
                    Item_Fat_Content string ,
                    Item_Visibility double ,
                    Item_Type string ,
                    Item_MRP double ,
                    Outlet_Identifier string ,
                    Outlet_Establishment_Year integer,
                    Outlet_Size string, 
                    Outlet_Location_Type string ,
                    Outlet_Type string ,
                    Item_Outlet_Sales double 
                 '''


# COMMAND ----------

# MAGIC %md
# MAGIC ## Assign previous datatype to present datatype changed

# COMMAND ----------

df = spark.read.format('csv')\
    .schema(my_ddl_schema)\
    .option('header',True)\
    .load('/FileStore/tables/BigMart_Sales.csv')
    


# COMMAND ----------

# MAGIC %md
# MAGIC ##Checking whether datatype changed or not(Item_weight->Double to string)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Re run read and print schema to get before datatypes only

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Select columns

# COMMAND ----------

df.select('Item_Identifier','Item_Weight','Item_Fat_Content').display()

###df.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content')).display()
##import col 

# COMMAND ----------

# MAGIC %md
# MAGIC df.select(col.'') as 

# COMMAND ----------

# MAGIC %md
# MAGIC #TRANSFORMATIONS
# MAGIC **Rename column/Alias**

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col('Item_Identifier').alias('Item_id')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Filtering
# MAGIC SCENARIO 1

# COMMAND ----------

df.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##SCENARIO 2

# COMMAND ----------

df.filter((col('Item_Fat_Content') == 'Regular') & (col('Item_Weight')<10)).display()
##df.filter((col('Item_Type') == 'Soft Drinks') & (col('Item_Weight')<10)).display()  
 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Scenario 3

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 2','Tier 1'))).display()
         

# COMMAND ----------

# MAGIC %md
# MAGIC #Rename column
# MAGIC with columnRenamed

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Add/Modify column**
# MAGIC
# MAGIC Scenario 1: Add New Column with constant value 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit

df = df.withColumn('flag',lit('new'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario 2 multiply two columns and get single column

# COMMAND ----------

df.withColumn('multiplied_Col',col('Item_Weight')*col('Item_MRP')).display()
     

# COMMAND ----------

# MAGIC %md
# MAGIC **Scenario 3: Modify existing column values**

# COMMAND ----------

from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions  import col 
df = df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'Low Fat','LF'))\
        .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'Regular','Reg'))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #import * so need not define every time

# COMMAND ----------

from pyspark.sql.types import * 
from pyspark.sql.functions import *  
     


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Casting

# COMMAND ----------

df= df.withColumn('Item_Weight',col('Item_Weight').cast(StringType()))


# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##SORTING
# MAGIC Scenario 1 : Sort in desc

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()


# COMMAND ----------

# MAGIC %md
# MAGIC scenaio 2 both in desc

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'],ascending= [0,0]).display()


# COMMAND ----------

# MAGIC %md
# MAGIC sceario 3: One is asc and one in desc

# COMMAND ----------

df.sort( ['Item_Weight','Item_Visibility'], ascending=[0,1] ) . display()


# COMMAND ----------

# MAGIC %md
# MAGIC ##limit

# COMMAND ----------

df.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##DROPING ROWS AND COLUMNS
# MAGIC Scenario 1 : Drop column

# COMMAND ----------

df.drop('Item_Visibility').display()

# COMMAND ----------

# MAGIC %md
# MAGIC scenario 2: Drop multiple columns

# COMMAND ----------

df.drop('Item_Visibility','Item_Type').display()

# COMMAND ----------

# MAGIC %md
# MAGIC Drop duplicate rows in entire dataset

# COMMAND ----------

# MAGIC %md
# MAGIC SCENARIO 3: Drop all duplicate rows

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

# MAGIC %md
# MAGIC SCENARIO 4 : Drop duplicate values in each column

# COMMAND ----------

df.dropDuplicates(subset=['Item_Type']).display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Subset is same as distinct- Subset is best

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Create two Dataframes

# COMMAND ----------

data1=[
    ('1','kad'),
    ('2','sid')
]
schema1= 'id string, name string '
df1 = spark.createDataFrame(data1,schema1)

data2=[('3','rahul'),
       ('4','jas')

]
schema2= 'id string, name string'
df2 = spark.createDataFrame(data2,schema2)

# COMMAND ----------

df1.display()
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #UNION
# MAGIC Put one after other

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating New Dataframes

# COMMAND ----------

data1 = [('kad','1',),
        ('sid','2',)]
schema1 = 'name STRING, id STRING' 

df1 = spark.createDataFrame(data1,schema1)

df1.display()
     

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #UnionByname
# MAGIC order of columns

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #String functions

# COMMAND ----------

# MAGIC %md
# MAGIC Upper
# MAGIC

# COMMAND ----------

df.select( upper('Item_Type').alias('New Upper case') ).display()


# COMMAND ----------

df.select( 'Item_Type',initcap('Item_Type').alias('Formatted Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Date Functions

# COMMAND ----------

# MAGIC %md
# MAGIC SCENARIO 1 : Add new columnn with todays date

# COMMAND ----------

# MAGIC %md
# MAGIC With assigning

# COMMAND ----------


df = df.withColumn('curr_date',current_date())

df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC Add

# COMMAND ----------

df = df.withColumn('week_after',date_add('curr_date',7))

df.display()

# COMMAND ----------

df = df.withColumn('week_before',date_add('curr_date',-7)) 

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Subtract

# COMMAND ----------

df.withColumn('week_before',date_sub('curr_date',7)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC SCENARIO 4 : DateDiff

# COMMAND ----------

df = df.withColumn('datediff',datediff('week_after','curr_date'))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC DateFormat- Y in samll

# COMMAND ----------


df = df.withColumn('week_before',date_format('week_before','dd-MM-yyyy'))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Handling NULL - DROP & FILL

# COMMAND ----------

# MAGIC %md
# MAGIC #Drop Null's
# MAGIC SCENARIO 1 : If any null value found remove entire row

# COMMAND ----------

df.dropna('any').display()

# COMMAND ----------

# MAGIC %md
# MAGIC SCENARIO 2 : Entire row should contain null values

# COMMAND ----------

df.dropna('all').display()

# COMMAND ----------

# MAGIC %md
# MAGIC SCENARIO 3 :Particular column which contain null remove those records

# COMMAND ----------

df.dropna( subset=['Outlet_Size'] ).display()

# COMMAND ----------


df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #FillNull's
# MAGIC SCENARIO 1 : fill those null with not valiable - only strings

# COMMAND ----------

df.fillna('NotAvailable').display()
#only fills strings

# COMMAND ----------

df.fillna(0.0,subset=['Item_Weight']).display()
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Split and indexing

# COMMAND ----------

# MAGIC %md
# MAGIC Split\
# MAGIC like a list
# MAGIC

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Index \
# MAGIC dont mention naming

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')[1]).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #Explode
# MAGIC SCENARIO 1 : First split and then explode one by one

# COMMAND ----------

df_exp = df.withColumn('Outlet_Type',split('Outlet_Type',' '))
df_exp.display()


# COMMAND ----------

df_exp.withColumn('Outlet_Type',explode('Outlet_Type')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC SCENARIO 2 : Assign Flag from a string

# COMMAND ----------

df_exp.withColumn( 'Type1_flag', array_contains('Outlet_Type','Type1') ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Group
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC SCENARIO 1 : Sales for each item
# MAGIC

# COMMAND ----------

#df.groupBy('Item_Type').sum('Item_MRP').display()
df.groupBy('Item_Type').agg(sum('Item_MRP').alias('Sales')).display()
     

# COMMAND ----------

# MAGIC %md
# MAGIC SCENARIO 2 :Group by on two columns

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC SCENARIO 3 - Two aggregation and two col

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP'),avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Collect_List

# COMMAND ----------

data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book4'),
        ('user3','book1')]

schema = 'user string, book string'

df_book = spark.createDataFrame(data,schema)


# COMMAND ----------

df_book.groupBy('user').agg(collect_list('book')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #PIVOT

# COMMAND ----------

df.groupBy('Item_Type').pivot('Outlet_size').agg(avg('Item_MRP')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #WHEN - OTHERWISE

# COMMAND ----------

df.select('Item_Type').distinct().display()


# COMMAND ----------

# MAGIC %md
# MAGIC Multiple Conditions

# COMMAND ----------

df = df.withColumn('Food_Status',when(col('Item_Type')=='Fruits and Vegetables','HEALTHY').otherwise('UN-HEALTHY')


# COMMAND ----------

df.withColumn('Food_Status',when((col('Item_Type') == 'Fruits and Vegetables') & (col('Item_MRP') < 150),
        'HEALTHY')\
        .when((col('Item_Type') == 'Meat') & (col('Item_MRP') >= 150), 'MODERATE')\
        .otherwise('UN-HEALTHY')).display()

     

# COMMAND ----------

# MAGIC %md
# MAGIC #Joins

# COMMAND ----------

# MAGIC %md
# MAGIC Create DataFrame

# COMMAND ----------

dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)

# COMMAND ----------

df1.display()
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Inner

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'inner').display()


# COMMAND ----------

# MAGIC %md
# MAGIC Left

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'left').display()

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC Anti Join -
# MAGIC except

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Window Functions

# COMMAND ----------


from pyspark.sql.window import Window


# COMMAND ----------

# MAGIC %md
# MAGIC Row_Number

# COMMAND ----------

df.withColumn( 'Row_Number', row_number().over(Window.orderBy('Item_Identifier') ) ).display()



# COMMAND ----------

# MAGIC %md
# MAGIC Rank

# COMMAND ----------

df.withColumn('Ranking',rank().over(Window.orderBy('Item_Identifier') ) ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC denserank()

# COMMAND ----------

df.withColumn('Ranking',dense_rank().over(Window.orderBy(col('Item_Identifier')) ) ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC rank Vs dense_rank

# COMMAND ----------

df.withColumn('rank',rank().over(Window.orderBy(col('Item_Identifier').desc())))\
        .withColumn('denseRank',dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display() 

# COMMAND ----------

# MAGIC %md
# MAGIC SCENARIO 1 : To all columns

# COMMAND ----------

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy ('Item_Type'))).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #Windows FRAME clause\
# MAGIC SCENARIO 2 : Pre

# COMMAND ----------



df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()
        

# COMMAND ----------

# MAGIC %md
# MAGIC SCENARIO 3 : Post

# COMMAND ----------


df.withColumn('totalsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #User Defined Function

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1

# COMMAND ----------

def myfunc(x):
    return x*x

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2

# COMMAND ----------

my_udf = udf(myfunc)

# COMMAND ----------

# MAGIC %md
# MAGIC Step 3

# COMMAND ----------

df.withColumn('Calculated_UDF',my_udf('Item_MRP') ).display()
#249.8092*249.8092

# COMMAND ----------

# MAGIC %md
# MAGIC #DATA WRITING
# MAGIC SCENARIO 1 ->CSV

# COMMAND ----------

df.write.format('csv')\
.save('./FileStore/tables/csv/Data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC SCENARIO 2 : append 

# COMMAND ----------

df.write.format('csv')\
        .mode('append')\
        .save('/FileStore/tables/CSV/Data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC OR THIS IS COMFORT

# COMMAND ----------

df.write.format('csv')\
        .mode('append')\
        .option('path','/FileStore/tables/CSV/Data.csv')\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC SCENARIO 3 : Truncate and load

# COMMAND ----------

df.write.format('csv')\
.mode('overwrite')\
.option('path','/FileStore/tables/CSV/Data.csv')\
.save()

# COMMAND ----------

# MAGIC %md
# MAGIC Error     

# COMMAND ----------

df.write.format('csv')\
.mode('error')\
.option('path','/FileStore/tables/CSV/Data.csv')\
.save()
     

# COMMAND ----------

# MAGIC %md
# MAGIC IGNORE
# MAGIC
# MAGIC

# COMMAND ----------

df.write.format('csv')\
.mode('ignore')\
.option('path','/FileStore/tables/CSV/Data.csv')\
.save()

# COMMAND ----------

# MAGIC %md
# MAGIC PARQUET

# COMMAND ----------

df.write.format('parquet')\
.mode('overwrite')\
.option('path','/FileStore/tables/CSV/Data.csv')\
.save()   

# COMMAND ----------

# MAGIC %md
# MAGIC #Table

# COMMAND ----------

df.write.format('parquet')\
.mode('overwrite')\
.saveAsTable('my_table')

# COMMAND ----------


df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #SPARK SQL

# COMMAND ----------

# MAGIC %md
# MAGIC creating TempView

# COMMAND ----------

df.createTempView('my_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC  select * from my_view where Item_Type='Dairy'

# COMMAND ----------

# MAGIC %md
# MAGIC Put sql in Dataframe again

# COMMAND ----------

df_sql = spark.sql(" select * from my_view where Item_Type='Dairy' ")

# COMMAND ----------


df_sql.display()