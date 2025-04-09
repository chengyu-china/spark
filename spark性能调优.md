## Spark 性能调优的思路

一个spark sql性能不好，我们按照网上的思路修改code，配置参数，发现不能提升性能，并不能说吗网上的办法是错误的，而是药不对症。需要找到影响性能的短板。
从硬件资源消耗的角度切入，往往是个不错的选择。我们都知道，从硬件的角度出发，计算负载划分为计算密集型、内存密集型和 IO 密集型。如果我们能够明确手中的应用属于哪种类型，自然能够缩小搜索范围，从而更容易锁定性能瓶颈。不过，在实际开发中，并不是所有负载都能明确划分出资源密集类型。比如说，Shuffle、数据关联这些数据分析领域中的典型场景，它们对 CPU、内存、磁盘与网络的要求都很高，任何一个环节不给力都有可能形成瓶颈。因此，就性能瓶颈定位来说，除了从硬件资源的视角出发，我们还需要关注典型场景。


## RDD 弹性分布式数据集

partitions，partitioner，dependences，compute 


## Spark 内存计算
在 Spark 中，内存计算有两层含义：第一层含义就是众所周知的分布式数据缓存，第二层含义是 Stage 内的流水线式计算模式。
Spark 允许开发者将分布式数据集缓存到计算节点的内存中，从而对其进行高效的数据访问。只有需要频繁访问的数据集才有必要 cache，对于一次性访问的数据集，cache 不但不能提升执行效率，反而会产生额外的性能开销，让结果适得其反。


第二层含义：Stage 内的流水线式计算模式。要弄清楚内存计算的第二层含义，咱们得从 DAG 的 Stages 划分说起
DAG 全称 Direct Acyclic Graph，中文叫有向无环图。顾名思义，DAG 是一种“图”。我们知道，任何一种图都包含两种基本元素：顶点（Vertex）和边（Edge），顶点通常用于表示实体，而边则代表实体间的关系。在 Spark 的 DAG 中，顶点是一个个 RDD，边则是 RDD 之间通过 dependencies 属性构成的父子关系。


以 Actions 算子为起点，从后向前回溯 DAG，以 Shuffle 操作为边界去划分 Stages。 在 Spark 中，流水线计算模式指的是：在同一 Stage 内部，所有算子融合为一个函数，Stage 的输出结果由这个函数一次性作用在输入数据集而产生，在内存中不产生任何中间数据形态。

由于计算的融合只发生在 Stages 内部，而 Shuffle 是切割 Stages 的边界，因此一旦发生 Shuffle，内存计算的代码融合就会中断。


控制任务并行度的参数是 Spark 的配置项：spark.default.parallelism。


## Spark 调度系统

先将用户构建的 DAG 转化为分布式任务，结合分布式集群资源的可用性，基于调度规则依序把分布式任务分发到执行器。

Spark 调度系统的工作流程包含如下 5 个步骤：
1. 将 DAG 拆分为不同的运行阶段 Stages；
2. 创建分布式任务 Tasks 和任务组 TaskSet；
3. 获取集群内可用的硬件资源情况；
4. 按照调度规则决定优先调度哪些任务 / 组；
5. 依序将分布式任务分发到执行器 Executor。

Spark 调度系统包含 3 个核心组件，分别是 DAGScheduler、TaskScheduler 和 SchedulerBackend。


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

sort merge join 妆化成 broadcast hash join .  使用hash 算法，把join key 穿换成 一列hash 值，这样可以让小表的数据了减少到 broadcast join 的广播阈值，从而提升效率

