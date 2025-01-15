本文还是以一个例子来带大家了解 spark sql。无需仔细研究，先随便看看即可。

```
import org.apache.spark.sql.DataFrame
 
val rootPath: String = ./exampledata/'2011-2019小汽车摇号数据'
# 申请者数据
val hdfs_path_apply: String = s"${rootPath}/apply"
# spark是spark-shell中默认的SparkSession实例
# 通过read API读取源文件
val applyNumbersDF: DataFrame = spark.read.parquet(hdfs_path_apply)
 
# 中签者数据
val hdfs_path_lucky: String = s"${rootPath}/lucky"
# 通过read API读取源文件
val luckyDogsDF: DataFrame = spark.read.parquet(hdfs_path_lucky)
 
# 过滤2016年以后的中签数据，且仅抽取中签号码carNum字段
val filteredLuckyDogs: DataFrame = luckyDogsDF.filter(col("batchNum") >= "201601").select("carNum")
 
# 摇号数据与中签数据做内关联，Join Key为中签号码carNum
val jointDF: DataFrame = applyNumbersDF.join(filteredLuckyDogs, Seq("carNum"), "inner")
 
# 以batchNum、carNum做分组，统计倍率系数
val multipliers: DataFrame = jointDF.groupBy(col("batchNum"),col("carNum"))
.agg(count(lit(1)).alias("multiplier"))
 
# 以carNum做分组，保留最大的倍率系数
val uniqueMultipliers: DataFrame = multipliers.groupBy("carNum")
.agg(max("multiplier").alias("multiplier"))
 
# 以multiplier倍率做分组，统计人数
val result: DataFrame = uniqueMultipliers.groupBy("multiplier")
.agg(count(lit(1)).alias("cnt"))
.orderBy("multiplier")
 
result.collect

```

## DaraFrame

SparkContext 通过 textFile API 把源数据转换为 RDD，而SparkSession通过read API把原数据转换成 DataFrame。DataFrame 和 RDD 有什么区别？
1. 与 RDD 一样，DataFrame 也用来封装分布式数据集，它也有数据分区的概念，也是通过算子来实现不同 DataFrame 之间的转换，只不过 DataFrame 采用了一套与 RDD 算子不同的独立算子集。
2. 在数据内容方面，与 RDD 不同，DataFrame 是一种带 Schema 的分布式数据集，因此，你可以简单地把 DataFrame 看作是数据库中的一张二维表。
3. DataFrame 背后的计算引擎是 Spark SQL，而 RDD 的计算引擎是 Spark Core。

### 创建DataFrame

创建 DataFrame 的方法有很多，Spark 支持多种数据源，按照数据来源进行划分，这些数据源可以分为如下几个大类：Driver 端自定义的数据结构、（分布式）文件系统、关系型数据库 RDBMS、关系型数据仓库、NoSQL 数据库，以及其他的计算引擎。
![DataFrameSource](./pictures/DataFrameSource.webp)

**从 Driver 创建 DataFrame**

前文提到 DataFrame相当于带有schema的RDD。所以 创建 DataFrame 的第一种方法，就是先创建 RDD，然后再给它加上Schema。这种方法比较的复杂，有如下四个步骤。
1. 使用spark context 创建 RDD
2. 创建schema
3. 把rdd转化成rdd[ROW]
4. 使用spark.createDataFrame(rowRDD,schema) 创建DataFrame 

```
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame

# 创建RDD
val seq: Seq[(String, Int)] = Seq(("Bob", 14), ("Alice", 18))
val rdd: RDD[(String, Int)] = sc.parallelize(seq)
## 创建schema
val schema:StructType = StructType( Array(
StructField("name", StringType),
StructField("age", IntegerType)
))
# createDataFrame 要求 RDD 的类型必须是 RDD[Row]。
val rowRDD: RDD[Row] = rdd.map(fileds => Row(fileds._1, fileds._2))

# 创建DataFrame
val dataFrame: DataFrame = spark.createDataFrame(rowRDD,schema)

```

**toDF 方法**

我们可以引入 spark.implicits 轻松实现RDD 到DataFrame 的转换，甚至是从seq 到DataFrame的转换。

```

import spark.implicits._

# 创建RDD
val seq: Seq[(String, Int)] = Seq(("Bob", 14), ("Alice", 18))
val rdd: RDD[(String, Int)] = sc.parallelize(seq)

val dataFrame: DataFrame = rdd.toDF
dataFrame.show

val dataFrame1: DataFrame = seq.toDF
dataFrame1.show

val dataFrame2: DataFrame = rdd.toDF("Name","Age")
dataFrame2.show

```

**从文件系统创建 DataFrame**

这是一种更为常用的方法，spark支持的文件系统非常多，无论是哪一种文件系统，spark 都要通过SparkSession 的 read API 来读取数据并创建 DataFrame。如下图所示：

![SparkSessionReadAPI](./pictures/SparkSessionReadAPI.webp)

1. 第 1 类参数文件格式。如 CSV、Text、Parquet、ORC、JSON等。
2. 第 2 类参数加载选项的可选集合。如CSV 文件格式可以通过 option(“header”, true)，来表明 CSV 文件的首行为 Data Schema。当需要指定多个选项时，我们可以用“option(选项 1, 值 1).option(选项 2, 值 2)”的方式来实现。
3. 第 3 类参数是文件路径。 

**从 CSV 创建 DataFrame**

从 CSV 文件成功地创建 DataFrame，关键在于了解并熟悉与之有关的option选项。
![CSVOptiion](./pictures/CSVOption.webp)

1. header: boolen 类型，默认为false。如果是false的话，DataFrame的数据列名是 _c0,_c1... 这种情况下如果想指定列名，可以定义schema，并调用schema方法，后面code中有演示。
2. seq: 它是用于分隔列数据的分隔符，可以是任意字符串，默认值是逗号。 常见的分隔符还有 Tab、“|”。
3. “escape”和“nullValue”分别用于指定文件中的转义字符和空值。
4. “dateFormat”则用于指定日期格式，默认值是“yyyy-MM-dd”。
5. “mode”，它用来指定文件的读取模式，更准确地说，它明确了 Spark SQL 应该如何对待 CSV 文件中的“脏数据”。mode 支持 3 个取值，分别是 permissive、dropMalformed 和 failFast。如下图所示。
![CSVMode](./pictures/SparkSessionReadCSVMode.webp)

```
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}

# 定义schema

val schema:StructType = StructType( Array(
StructField("name", StringType),
StructField("age", IntegerType)
))

val csvFilePath: String = "./exampledata/sparksessionreadcsv.csv"

val df: DataFrame = spark.read.format("csv").schema(schema).option("header", false).load(csvFilePath)

df.show


val csvFilePath: String = _
val df: DataFrame = spark.read.format("csv")
.schema(schema)
.option("header", true)
.option("mode", "dropMalformed")
.load(csvFilePath)
// df: org.apache.spark.sql.DataFrame = [name: string, age: int]
df.show


```

**从 Parquet / ORC 创建 DataFrame**

读取这两种文件比较简单，因为parquet / ORC 都是列存储文件，文件里面便存有schema的信息。因此直接读取就行。

```
val parquetFilePath: String = _
val df: DataFrame = spark.read.format("parquet").load(parquetFilePath)

val orcFilePath: String = _
val df: DataFrame = spark.read.format("orc").load(orcFilePath)
```

**从 RDBMS 创建 DataFrame**

以mysql 为例子，我们可以使用很多option 来实现从 mysql 创建 DataFrame。 在默认情况下，Spark 安装目录并没有提供与数据库连接有关的任何 Jar 包，因此，对于想要访问的数据库，不论是 MySQL、PostgreSQL，还是 Oracle、DB2，我们都需要把相关 Jar 包手工拷贝到 Spark 安装目录下的 Jars 文件夹。 
还要在 spark-shell 命令或是 spark-submit 中，通过如下两个命令行参数，来告诉 Spark 相关 Jar 包的访问地址。
–driver-class-path mysql-connector-java-version.jar
–jars mysql-connector-java-version.jar

```
spark.read.format("jdbc")
.option("driver", "com.mysql.jdbc.Driver")
.option("url", "jdbc:mysql://hostname:port/mysql")
.option("user", "用户名")
.option("password","密码")
.option("numPartitions", 20) # 控制并行度，即 DataFrame 的 Partitions 数量
.option("dbtable", "数据表名 ") # 也可以传递sql  .option("dbtable", sqlQuery)
.load()

```

## DataFrame 数据处理

DataFrame 提供了两种方法做数据处理，sql 和 DataFrame开发算子。 

### sql 语言
对于任意的 DataFrame，我们都可以使用 createTempView 或是 createGlobalTempView 在 Spark SQL 中创建临时数据表。有了临时表之后就可以使用sql语言处理数据。 

```
import org.apache.spark.sql.DataFrame
import spark.implicits._
 
val seq = Seq(("Alice", 18), ("Bob", 14))
val df = seq.toDF("name", "age")
 
df.createTempView("t1")
val query: String = "select * from t1"
// spark为SparkSession实例对象
val result: DataFrame = spark.sql(query)
 
result.show
 
```

### DataFrame 算子
DataFrame 的算子是非常多的，简单分类如下。
![DataFrame算子](./pictures/DataFrame算子.webp)

**同源类算子**

DataFrame 来自 RDD，与 RDD 具有同源性，因此 RDD 支持的大部分算子，DataFrame 都支持。
![同源类算子](./pictures/DataFrame同源算子.webp)

**探索类算子**

这类算子的作用，在于帮助开发者初步了解并认识数据，比如数据的模式（Schema）、数据的分布、数据的“模样”，等等。
![探索类算子](./pictures/DataFrame探索类算子.webp)

```
import org.apache.spark.sql.DataFrame
import spark.implicits._
 
val employees = Seq((1, "John", 26, "Male"), (2, "Lily", 28, "Female"), (3, "Raymond", 30, "Male"))
val employeesDF: DataFrame = employees.toDF("id", "name", "age", "gender")
 
employeesDF.printSchema
 
```

**清洗类算子**

![清洗类算子](./pictures/DataFrame清洗类算子.webp)

1. drop 算子允许开发者直接把指定列从 DataFrame 中予以清除。 employeesDF.drop(“gender”)
2. distinct，它用来为 DataFrame 中的数据做去重。 employeesDF.distinct
3. dropDuplicates，它的作用也是去重。不过，与 distinct 不同的是，dropDuplicates 可以指定数据列. employeesDF.dropDuplicates("gender").
4. na，它的作用是选取 DataFrame 中的 null 数据，na 往往要结合 drop 或是 fill 来使用。employeesDF.na.drop 用于删除 DataFrame 中带 null 值的数据记录。而 employeesDF.na.fill(0) 则将 DataFrame 中所有的 null 值都自动填充为整数零。

**转换类算子**

转换类算子的主要用于数据的生成、提取与转换。

![转换类算子](./pictures/DataFrame转换类算子.webp)

1. select 算子让我们可以按照列名对 DataFrame 做投影，比如说，如果我们只关心年龄与性别这两个字段的话，就可以使用下面的语句来实现。
```
employeesDF.select("name", "gender").show
```
2. selectExpr 相较于select 更为灵活。
```
employeesDF.selectExpr("id", "name", "concat(id, '_', name) as id_name").show
 
/** 结果打印
+---+-------+---------+
| id| name| id_name|
+---+-------+---------+
| 1| John| 1_John|
| 2| Lily| 2_Lily|
| 3|Raymond|3_Raymond|
+---+-------+---------+
*/
```
3. where 是对 DataFrame 做数据过滤。
```
employeesDF.where(“gender = ‘Male’”)
```
4. withColumnRenamed 的作用是字段重命名
```
employeesDF.withColumnRenamed(“gender”, “sex”)。
```
5. withColumn 则用于生成新的数据列.
```
employeesDF.withColumn("crypto", hash($"age")).show
 
/** 结果打印
+---+-------+---+------+-----------+
| id| name|age|gender| crypto|
+---+-------+---+------+-----------+
| 1| John| 26| Male|-1223696181|
| 2| Lily| 28|Female|-1721654386|
| 3|Raymond| 30| Male| 1796998381|
+---+-------+---+------+-----------+
*/
```
6. explode,它的作用是展开数组类型的数据列，数组当中的每一个元素，都会生成一行新的数据记录。 爆炸函数。 
```
val seq = Seq( (1, "John", 26, "Male", Seq("Sports", "News")),
(2, "Lily", 28, "Female", Seq("Shopping", "Reading")),
(3, "Raymond", 30, "Male", Seq("Sports", "Reading"))
)
 
val employeesDF: DataFrame = seq.toDF("id", "name", "age", "gender", "interests")
employeesDF.show
 
/** 结果打印
+---+-------+---+------+-------------------+
| id| name|age|gender| interests|
+---+-------+---+------+-------------------+
| 1| John| 26| Male| [Sports, News]|
| 2| Lily| 28|Female|[Shopping, Reading]|
| 3|Raymond| 30| Male| [Sports, Reading]|
+---+-------+---+------+-------------------+
*/
 
employeesDF.withColumn("interest", explode($"interests")).show
 
/** 结果打印
+---+-------+---+------+-------------------+--------+
| id| name|age|gender| interests|interest|
+---+-------+---+------+-------------------+--------+
| 1| John| 26| Male| [Sports, News]| Sports|
| 1| John| 26| Male| [Sports, News]| News|
| 2| Lily| 28|Female|[Shopping, Reading]|Shopping|
| 2| Lily| 28|Female|[Shopping, Reading]| Reading|
| 3|Raymond| 30| Male| [Sports, Reading]| Sports|
| 3|Raymond| 30| Male| [Sports, Reading]| Reading|
+---+-------+---+------+-------------------+--------+
*/
```

**分析类算子**

在大多数的数据应用中，数据分析往往是最为关键的那环，甚至是应用本身的核心目的。
![分析类算子](./pictures/DataFrame分析类算子.webp)

为了演示上述算子的用法，我们先来准备两张数据表：employees 和 salaries，也即员工信息表和薪水表。我们的想法是，通过对两张表做数据关联，来分析员工薪水的分布情况。

```
import spark.implicits._
import org.apache.spark.sql.DataFrame
 
// 创建员工信息表
val seq = Seq((1, "Mike", 28, "Male"), (2, "Lily", 30, "Female"), (3, "Raymond", 26, "Male"))
val employees: DataFrame = seq.toDF("id", "name", "age", "gender")
 
// 创建薪水表
val seq2 = Seq((1, 26000), (2, 30000), (4, 25000), (3, 20000))
val salaries:DataFrame = seq2.toDF("id", "salary")
 
employees.show
 
/** 结果打印
+---+-------+---+------+
| id| name|age|gender|
+---+-------+---+------+
| 1| Mike| 28| Male|
| 2| Lily| 30|Female|
| 3|Raymond| 26| Male|
+---+-------+---+------+
*/
 
salaries.show
 
/** 结果打印
+---+------+
| id|salary|
+---+------+
| 1| 26000|
| 2| 30000|
| 4| 25000|
| 3| 20000|
+---+------+
*/

val jointDF: DataFrame = salaries.join(employees, Seq("id"), "inner")
 
jointDF.show
 
/** 结果打印
+---+------+-------+---+------+
| id|salary| name|age|gender|
+---+------+-------+---+------+
| 1| 26000| Mike| 28| Male|
| 2| 30000| Lily| 30|Female|
| 3| 20000|Raymond| 26| Male|
+---+------+-------+---+------+
*/

val aggResult = fullInfo.groupBy("gender").agg(sum("salary").as("sum_salary"), avg("salary").as("avg_salary"))
 
aggResult.show
 
/** 数据打印
+------+----------+----------+
|gender|sum_salary|avg_salary|
+------+----------+----------+
|Female| 30000| 30000.0|
| Male| 46000| 23000.0|
+------+----------+----------+
*/
aggResult.sort(desc("sum_salary"), asc("gender")).show
 
/** 结果打印
+------+----------+----------+
|gender|sum_salary|avg_salary|
+------+----------+----------+
| Male| 46000| 23000.0|
|Female| 30000| 30000.0|
+------+----------+----------+
*/
 
aggResult.orderBy(desc("sum_salary"), asc("gender")).show
 
/** 结果打印
+------+----------+----------+
|gender|sum_salary|avg_salary|
+------+----------+----------+
| Male| 46000| 23000.0|
|Female| 30000| 30000.0|
+------+----------+----------+
*/

```

**持久化类算子**

与read API 类似，write API 也有 3 个关键环节，分别是同样由 format 定义的文件格式，零到多个由 option 构成的“写入选项”，以及由 save 指定的存储路径，如下图所示。
![持久化类算子](./pictures/SparkSessionWriteAPI.webp)

在option方法中可以指定文件写入的mode， 文件写入mode 有 Append、Overwrite、ErrorIfExists、Ignore 这 4 种模式。

![sparkSessionWriteMode](./pictures/SparkSessionWriteMode.webp)


## spark sql 的关联
数据join的概念都比较熟悉，使用DataFrame实现join的命令如下图所示。 
![DataFrameJoiin](./pictures/DataFrameJoin.webp)

关联形式有很多种，如下图。
![DataFrameJoinType](./pictures/DataFrameJoinType.webp)

下面的code是使用DataFrame实现join的例子。

```
import spark.implicits._
import org.apache.spark.sql.DataFrame
 
# 创建员工信息表
val seq = Seq((1, "Mike", 28, "Male"), (2, "Lily", 30, "Female"), (3, "Raymond", 26, "Male"), (5, "Dave", 36, "Male"))
val employees: DataFrame = seq.toDF("id", "name", "age", "gender")

# 创建薪资表
val seq2 = Seq((1, 26000), (2, 30000), (4, 25000), (3, 20000))
val salaries:DataFrame = seq2.toDF("id", "salary")

# inner join 
// 内关联
val jointDF: DataFrame = salaries.join(employees, salaries("id") === employees("id"), "inner")
jointDF.show

# left join
val jointDF: DataFrame = salaries.join(employees, salaries("id") === employees("id"), "left")
jointDF.show
 
# right join
val jointDF: DataFrame = salaries.join(employees, salaries("id") === employees("id"), "right")
jointDF.show

# fullout join
val jointDF: DataFrame = salaries.join(employees, salaries("id") === employees("id"), "full")
jointDF.show

# left semo join. left join 但是只显示左表的column
val jointDF: DataFrame = salaries.join(employees, salaries("id") === employees("id"), "leftsemi")
jointDF.show

# Left Anti Join. left join 但是只保留左表的column，保留的数据是不满足join条件的数据。
val jointDF: DataFrame = salaries.join(employees, salaries("id") === employees("id"), "leftanti")
jointDF.show

```
### 关联机制 Spark Join Mechanisms

Join 有 3 种实现机制，分别是 NLJ（Nested Loop Join）、SMJ（Sort Merge Join）和 HJ（Hash Join）。

**NLJ（Nested Loop Join）** 
性能比较差。但是 像 salaries(“id”) < employees(“id”) 这样的关联条件，HJ 和 SMJ 是无能为力的。相反，NLJ 既可以处理等值关联（Equi Join），也可以应付不等值关联（Non Equi Join）。
所以是不是说明在join条件中最好不要出现 '>' '<' '>=' '<=' '!=' 等等？

**SMJ（Sort Merge Join）**

Sort Merge Join 首先会对数据进行排序。Sort Merge Join 就没有内存方面的限制。不论是排序、还是合并，SMJ 都可以利用磁盘来完成计算。所以，在稳定性这方面，SMJ 更胜一筹。如果准备参与 Join 的两张表是有序表，那么这个时候采用 SMJ 算法来实现关联简直是再好不过了。

**HJ（Hash Join）**

Hash Join 的执行效率最高，不过需要先在内存中构建出哈希表，对于内存的要求比较高，适用于内存能够容纳基表(右表)数据的计算场景。

### 分布式环境下的join
在分布式环境中，我们需要保证在join的时候，左表和右边满足join keys的记录在同一个executor中。能满足这个前提的途径只有两个：Shuffle 和 Broadcast。 

**shuffle join**

默认情况下，spark 使用的是 shuffle join 来完成分布式环境的数据关联。 根据Join Keys 计算哈希值，并将哈希值对并行度进行取模 便可以保证左表和右边满足join keys的记录在同一个executor中。 然后在reduce 阶段，Reduce Task 就可以使用 HJ、SMJ、或是 NLJ 算法在 Executors 内部完成数据关联的计算。

**broadcast join**

就是把小表广播出去。仅仅分发体量较小的数据表来完成数据关联，执行性能显然要高效得多。

```
import org.apache.spark.sql.functions.broadcast
 
// 创建员工表的广播变量
val bcEmployees = broadcast(employees)
 
// 内关联，PS：将原来的employees替换为bcEmployees
val jointDF: DataFrame = salaries.join(bcEmployees, salaries("id") === employees("id"), "inner")
```

不论是 Shuffle Join，还是 Broadcast Join，一旦数据分发完毕，理论上可以采用 HJ、SMJ 和 NLJ 这 3 种实现机制中的任意一种，完成 Executors 内部的数据关联。 
两种分发模式，与三种实现机制，它们组合起来，总共有 6 种分布式 Join 策略。spark 支持其中5种。如下图蓝色部分。
![sparkJoin策略](./pictures/SparkJoin策略.webp).

对于等值关联（Equi Join），Spark SQL 优先考虑采用 Broadcast HJ 策略，其次是 Shuffle SMJ，最次是 Shuffle HJ。对于不等值关联（Non Equi Join），Spark SQL 优先考虑 Broadcast NLJ，其次是 Shuffle NLJ。不论是等值关联、还是不等值关联，只要 Broadcast Join 的前提条件成立，Spark SQL 一定会优先选择 Broadcast Join 相关的策略
![join策略对比](./pictures/join策略对比.webp)

Broadcast join 的性能是最好的，但是不是所有的join 都能使用broascast join。 我们知道 只有Driver 可以把小表广播出去，因此Broadcast join 需要Driver 有足够的内存。 但是如果需要被广播的表太大的话，广播这个表的时间就会很久。所以我们一般是广播小表。 我们之前在工作中有使用调参的形式，出发broadcast join。


## spark sql 参数配置
