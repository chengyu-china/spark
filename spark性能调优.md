## Spark 性能调优的思路

性能调优不是一锤子买卖，补齐一个短板，其他板子可能会成为新的短板。因此，它是一个动态、持续不断的过程。
性能调优的手段和方法是否高效，取决于它针对的是木桶的长板还是瓶颈。针对瓶颈，事半功倍；针对长板，事倍功半。
性能调优的方法和技巧，没有一定之规，也不是一成不变，随着木桶短板的此消彼长需要相应的动态切换。
性能调优的过程收敛于一种所有木板齐平、没有瓶颈的状态。

一个spark sql性能不好，我们按照网上的思路修改code，配置参数，发现不能提升性能，并不能说吗网上的办法是错误的，而是药不对症。需要找到影响性能的短板。
从硬件资源消耗的角度切入，往往是个不错的选择。我们都知道，从硬件的角度出发，计算负载划分为计算密集型、内存密集型和 IO 密集型。
如果我们能够明确手中的应用属于哪种类型，自然能够缩小搜索范围，从而更容易锁定性能瓶颈。不过，在实际开发中，并不是所有负载都能明确划分出资源密集类型。比如说，Shuffle、数据关联这些数据分析领域中的典型场景，它们对 CPU、内存、磁盘与网络的要求都很高，任何一个环节不给力都有可能形成瓶颈。因此，就性能瓶颈定位来说，除了从硬件资源的视角出发，我们还需要关注典型场景。

Spark 的性能调优可以从应用代码和 Spark 配置项这 2 个层面展开


## RDD 弹性分布式数据集

partitions，partitioner，dependences，compute 


## Spark 内存计算
在 Spark 中，内存计算有两层含义：第一层含义就是众所周知的分布式数据缓存，第二层含义是 Stage 内的流水线式计算模式。

Spark 允许开发者将分布式数据集缓存到计算节点的内存中，从而对其进行高效的数据访问。只有需要频繁访问的数据集才有必要 cache，对于一次性访问的数据集，cache 不但不能提升执行效率，反而会产生额外的性能开销，让结果适得其反。


第二层含义：Stage 内的流水线式计算模式。要弄清楚内存计算的第二层含义，咱们得从 DAG 的 Stages 划分说起
DAG 全称 Direct Acyclic Graph，中文叫有向无环图。顾名思义，DAG 是一种“图”。我们知道，任何一种图都包含两种基本元素：顶点（Vertex）和边（Edge），顶点通常用于表示实体，而边则代表实体间的关系。在 Spark 的 DAG 中，顶点是一个个 RDD，边则是 RDD 之间通过 dependencies 属性构成的父子关系。


以 Actions 算子为起点，从后向前回溯 DAG，以 Shuffle 操作为边界去划分 Stages。 
在 Spark 中，流水线计算模式指的是：在同一 Stage 内部，所有算子融合为一个函数，Stage 的输出结果由这个函数一次性作用在输入数据集而产生，在内存中不产生任何中间数据形态。

由于计算的融合只发生在 Stages 内部，而 Shuffle 是切割 Stages 的边界，因此一旦发生 Shuffle，内存计算的代码融合就会中断。




## Spark 调度系统
控制任务并行度的参数是 Spark 的配置项：spark.default.parallelism。

先将用户构建的 DAG 转化为分布式任务，结合分布式集群资源的可用性，基于调度规则依序把分布式任务分发到执行器。

Spark 调度系统的工作流程包含如下 5 个步骤：
1. 将 DAG 拆分为不同的运行阶段 Stages；
2. 创建分布式任务 Tasks 和任务组 TaskSet；
3. 获取集群内可用的硬件资源情况；
4. 按照调度规则决定优先调度哪些任务 / 组；
5. 依序将分布式任务分发到执行器 Executor。

Spark 调度系统包含 3 个核心组件，分别是 DAGScheduler、TaskScheduler 和 SchedulerBackend。

## Spark调优 资源配置 CPU、内存、磁盘有关的配置项

CPU: 

spark.cores.max   集群范围内满配CPU核数
spark.executor.cores   单个executor 内CPU核数
spark.task.cpus   单个任务消耗的CPU核数
spark.default.parallelism   未指定分区数时默认的并行度
spark.sql.shuffle.partitions  数据关联，聚合操作中reduce 的并行度

并行度和并行计算是有区别的
并行度的出发点是数据，它明确了数据划分的粒度。并行度越高，数据的粒度越细，数据分片越多，数据越分散。由此可见，像分区数量、分片数量、Partitions 这些概念都是并行度的同义词。

并行计算任务则不同，它指的是在任一时刻整个集群能够同时计算的任务数量

内存:

spark.memory.fraction  堆内内存中有多大比例可供spark 消耗 
1 - spark.memory.fraction 就是user Memory 所占比例。User Memory 存储的主要是开发者自定义的数据结构，这些数据结构往往用来协助分布式数据集的处理。 因此当用户自定义的数据结构很少时，可以增加spark.memory.fraction。



堆内内存：Reserved Memory、User Memory、Execution Memory 和 Storage Memory



堆外内存：Execution Memory 和 Storage Memory 
前提：set spark.memory.offHeap.enabled=true


对于需要处理的数据集，如果数据模式比较扁平，而且字段多是定长数据类型，就更多地使用堆外内存。相反地，如果数据模式很复杂，嵌套结构或变长字段很多，就更多采用 JVM 堆内内存会更加稳妥。

Execution Memory：用于执行时，可以抢占Storage memory。 
Storage Memory：用于缓存时。 如果需要用到缓存，cache temporary view 时



## Spark 优化的 shuffle 配置
spark.shuffle.file.buffer： Map 输出端的写入缓冲区大小
spark.reducer.maxSizeInFlight：Reduce 输入端的读缓冲区的大小。 

spark.shuffle.sort.bypassMergeThreshold： 自 1.6 版本之后，Spark 统一采用 Sort shuffle manager 来管理 Shuffle 操作。无论计算结果本身是否需要排序，Shuffle 计算过程在 Map 阶段和 Reduce 阶段都会引入排序操作。 这种机制对于无需排序的操作很不公平（group by/repartition）。在不需要聚合，也不需要排序的计算场景中，我们就可以通过设置 spark.shuffle.sort.bypassMergeThreshold 的参数，来改变 Reduce 端的并行度（默认值是 200）。当 Reduce 端的分区数小于这个设置值的时候，我们就能避免 Shuffle 在计算过程引入排序。


## Spark sql 的相关配置
AQE： spark.sql.adaptive.enabled=true 来开启 AQE。自动分区合并、自动数据倾斜处理和 Join 策略

自动分区合并: AQE 事先并不判断哪些分区足够小，而是按照分区编号进行扫描，当扫描量超过“目标尺寸”时，就合并一次.
spark.sql.adaptive.advisoryPartitionSizeInBytes 设置目标分区大小
spark.sql.adaptive.coalescePartitions.minPartitionNum 合并分区后最小分区数。 避免一直合并导致并行度过低，使CPU 资源利用不充分。

自动数据倾斜：把大的数据分区拆分成小的数据数据分区
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes 数据分区的阈值
spark.sql.adaptive.skewJoin.skewedPartitionFactor 判断数据分区是否倾斜的比例系数。放大倍数。
分区大小>max{阈值，中位数*因子} 满足上述条件的分区需要拆分
spark.sql.adaptive.advisoryPartitionSizeInBytes 数据倾斜的分区，按advisoryPartitionSizeInBytes 大小进行拆分

Join 策略：AQE 会在数据表完成过滤操作之后动态计算剩余数据量，当数据量满足广播条件时，AQE 会重新调整逻辑执行计划，在新的逻辑计划中把 Shuffle Joins 降级为 Broadcast Join。再者，运行时的数据量估算要比编译时准确得多，因此 AQE 的动态 Join 策略调整相比静态优化会更可靠、更稳定。

spark.sql.autoBroadcastJoinThreshold 默认是10M 可以设置成2G




## Sort shuffle manager 




## 用 Join Hints 强制广播

select /*+ broadcast(t2) */ 
* 
from t1 inner join t2 on t1.key = t2.key

首先，从性能上来讲，Driver 在创建广播变量的过程中，需要拉取分布式数据集所有的数据分片。在这个过程中，网络开销和 Driver 内存会成为性能隐患。广播变量尺寸越大，额外引入的性能开销就会越多。更何况，如果广播变量大小超过 8GB，Spark 会直接抛异常中断任务执行。其次，从功能上来讲，并不是所有的 Joins 类型都可以转换为 Broadcast Joins。一来，Broadcast Joins 不支持全连接（Full Outer Joins）；二来，在所有的数据关联中，我们不能广播基表。或者说，即便开发者强制广播基表，也无济于事。比如说，在左连接（Left Outer Join）中，我们只能广播右表；在右连接（Right Outer Join）中，我们只能广播左表。在下面的代码中，即便我们强制用 broadcast 函数进行广播，Spark SQL 在运行时还是会选择 Shuffle Joins。


## CPU 与 内存

要想平衡 CPU 与执行内存之间的协同和配比，我们需要使用 3 类配置参数，它们分别控制着并行度、执行内存大小和集群的并行计算能力。

并行度、并发度与执行内存

并行度明确了数据划分的粒度：并行度越高，数据的粒度越细，数据分片越多，数据越分散。并行度可以通过两个参数来设置，分别是 spark.default.parallelism 和 spark.sql.shuffle.partitions。

并发度： Executor 的线程池大小由参数 spark.executor.cores 决定。 spark.task.cpus 默认数值为 1，并发度基本由 spark.executor.cores 参数敲定。

在给定执行内存 M、线程池大小 N 和数据总量 D 的时候，想要有效地提升 CPU 利用率，
我们就要计算出最佳并行度 P，
计算方法是让数据分片的平均大小 D/P 坐落在（M/N/2, M/N）区间。这样，在运行时，我们的 CPU 利用率往往不会太差。

executor memory 6G
executor core 2

需要处理的数据大小是 50G

合适的并行度是多少？ 

数据分区大小 是 [1.5,3] 我们可以取 2 。 

50G/2 = 25。

所以可以设置 spark.default.parallelism = 25 
和 spark.sql.shuffle.partitions = 25


## cache 的合理使用


## OOM 
发生 OOM 的 LOC（Line Of Code），也就是代码位置在哪？
OOM 发生在 Driver 端，还是在 Executor 端？
如果是发生在 Executor 端，OOM 到底发生在哪一片内存区域？


使用broadcast join的时候，有可能到值driver 端的OOM。 ```java.lang.OutOfMemoryError: Not enough memory to build and broadcast```
spark.driver.memory


数据倾斜的处理有几种： 
1.降低core，这样的话虽然并发度将了，但是每个任务内存上限可以提升，可以容纳计算内存 
2.增大内存，保持并发度和并行度不变的情况下，增大内存，将1/N的上限提高 
3.打散数据，让所有的分片数据尺寸降低，不过这个AQE自适应里头就会做




## catalyst 逻辑计划解析和逻辑计划优化
优化规则：
   - 谓词下推（Predicate Pushdown）: “谓词”指代的是像用户表上“age < 30”这样的过滤条件，“下推”指代的是把这些谓词沿着执行计划向下，推到离数据源最近的地方，从而在源头就减少数据扫描量。
     谓词下推都会有的，但是仅仅对带有统计信息的文件格式有明显效果，下推的谓词能够大幅减少数据扫描量，降低磁盘 I/O 开销。
   - 列剪裁（Column Pruning）： 列剪裁就是扫描数据源的时候，只读取那些与查询相关的字段。以小 Q 为例，用户表的 Schema 是（userId、name、age、gender、email），但是查询中压根就没有出现过 email 的引用，因此，Catalyst 会使用 ColumnPruning 规则，把 email 这一列“剪掉”。经过这一步优化，Spark 在读取 Parquet 文件的时候就会跳过 email 这一列，从而节省 I/O 开销。
   - 常量替换 （Constant Folding）：它的逻辑比较简单。假设我们在年龄上加的过滤条件是“age < 12 + 18”，Catalyst 会使用 ConstantFolding 规则，自动帮我们把条件变成“age < 30”。再比如，我们在 select 语句中，掺杂了一些常量表达式，Catalyst 也会自动地用表达式的结果进行替换。
   - Cache Manager 优化： 这里的 Cache 指的就是我们常说的分布式数据缓存。想要对数据进行缓存，你可以调用 DataFrame 的.cache 或.persist，或是在 SQL 语句中使用“cache table”关键字。


## 物理优化阶段

这个“典型”至少要满足两个标准：一，它要在我们的应用场景中非常普遍；二，它的取舍对于执行性能的影响最为关键。 JoinSelection 


## AQE 
AQE 是 Spark SQL 的一种动态优化机制，在运行时，每当 Shuffle Map 阶段执行完毕，AQE 都会结合这个阶段的统计信息，基于既定的规则动态地调整、修正尚未执行的逻辑计划和物理计划，来完成对原始查询语句的运行时优化。
AQE 优化机制触发的时机是 Shuffle Map 阶段执行完毕。也就是说，AQE 优化的频次与执行计划中 Shuffle 的次数一致。反过来说，如果你的查询语句不会引入 Shuffle 操作，那么 Spark SQL 是不会触发 AQE 的。


join 策略调整，合并分区，自动倾斜处理。

join 策略调整：

spark.sql.autoBroadcastJoinThreshold
spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin
spark.sql.adaptive.localShuffleReader.enabled

合并分区：
spark.sql.adaptive.advisoryPartitionSizeInBytes
spark.sql.adaptive.coalescePartitions.minPartitionNum

自动倾斜处理：
spark.sql.adaptive.skewJoin.skewedPartitionFactor
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes
spark.sql.adaptive.advisoryPartitionSizeInBytes


## 动态分区裁剪 Dynamic Partition Pruning

DPP 指的是在数据关联的场景中，Spark SQL 利用维度表提供的过滤信息，减少事实表中数据的扫描量、降低 I/O 开销，从而提升执行性能。 DPP 正是基于上述逻辑，把维度表中的过滤条件，通过关联关系传导到事实表，从而完成事实表的优化。事实表必须是分区表，而且分区字段（可以是多个）必须包含 Join Key。

DPP 机制得以实施还有一个隐含的条件：维度表过滤之后的数据集要小于广播阈值。


sort merge join 转化成 broadcast hash join .  
使用hash 算法，把join key 穿换成 一列hash 值，
这样可以让小表的数据了减少到 broadcast join 的广播阈值，从而提升效率

