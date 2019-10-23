# 插件开发


## 插件体系介绍

本文基于waterdrop 开发，需要提前了解waterdrop的一些特性

主要增加mysql的各个场景，包括多并发抽取，分表抽取，数据倾斜的表抽取，根据createtime做where条件的抽取。
并且自动创建表（分区），指定location。默认parquet格式
默认只使用double bigint string三种数据格式

抽取需要进一步完善的功能：

- 解决部分异常不能正确抛出的问题
- 支持 hive 建表 with comments from mysql
- 

技术改善点：

1. select * 改成显式指定 columns, 避免业务库加字段导致 hive 表错位
2. 抽取结果数据格式由 TEXT 改为 Parquet, 提升查询效率和缩短下游依赖任务运行时间，如 xxxx_fiction_like 任务运行时间由原先 2 个半小时缩短为不足半小时
3. 所有 numeric 字段映射为 hive 的 bigint,与double 避免了因为业务库将 int 字段类型修改为 bigint 而导致的 hive 读取报错
4. 解决了 dataflow 抽取建时，遇到保留字段名如 desc -> unknown，将 hive 表字段转成 unknown 问题
5. 采用了更高效率的抽取工具 waterdrop, 底层使用 spark 计算引擎，并行抽取，提升了入库效率
6. 改进了 dataflow 抽取只生成单个文件，导致读取只能使用一个 mapper, 读取效率低
7. 新增支持多种JDBC数据源，mysql osql psql。
其它方面：

1. 统一了抽取工具和调度系统，以往抽取任务分散，分别有 gobbin, dataflow, airflow, 现在统使用 waterdrop 抽取，airflow 调度，抽取任务与其它数仓任务可以统一管理
2. 抽取任务统一迁移到了新的 hadoop 集群，在迁移过程中梳理并下线了一些无用的抽取，减少了资源消耗
3. 在迁移过程中纠正了部分 gobblin 成功标志时间的问题，gobblin 使用当前时间，改为调度时间，以减少数据延迟重跑导致的成功标志错误
4. 抽取任务，脚本和表名采用了最的数仓规范，规范了抽取任务，更便于管理
5. 迁移过程中将许多主库抽取的任务改为从库抽取，减少了抽取对线上业务系统的影响
6. 与 DBA 一起统一了抽取帐号，以往每个入库需求都需要 DBA 开单独的帐号，效率低下，且难于维护，新需求统一使用 ods_read, 统一帐号也为后期数据源管理和数据集成系统提供了便利
7.更新后支持同步mysql的comment信息
未解决的点:
1.更改字段比较麻烦。




### 支持的场景

- 分表从mysql-hive
- mysql-hive 两种并发方式(索引是否跨度大)
- hive-redis 支持redis两种数据类型 string 和 hash
- hive-clickhouse 

下面介绍一下自定义的插件

### 1.org.interestinglab.waterdrop.input.MyJdbcById

用法：该组建的作用是通过ID分组，提高并发抽取mysql数据量。不适用于索引跨度比较大。

```java
input {
org.interestinglab.waterdrop.input.MyJdbc {
#mysql地址
url="jdbc:mysql://rm-xxxx.mysql.rds.aliyuncs.com/xxx?zeroDateTimeBehavior=convertToNull"
#想要同步的表
table="xxx"
#默认参数
table_name="xxx"
#将colmn分成多少份数据如果有特别大的数据倾斜不太好使
repartition="100"
#必填整型的自增列
column="id"
user="xxx"
password="xxx"
#默认参数
result_table_name="access_log"
}
}
```

### 2. org.interestinglab.waterdrop.input.SubTable 与filter org.interestinglab.waterdrop.filter.SubTableSql 相结合使用 用来从mysql的多表同步到Hive

用法：主要是配置上相同步的mysql数据表的正则表达式，将表查询出来之后循环插入

```java
input {
   org.interestinglab.waterdrop.input.SubTable {
        url = "${mysql_url}"
        database = "${mysql_db}"
        user = "${mysql_user}"
        password = "${mysql_password}"
        result_table_name = "access_log"
        //mysql的正则
        tableRegexp = "${mysql_table_prefix}"
  }
}

filter {
   org.interestinglab.waterdrop.filter.SubTableSql {
      url = "${mysql_url}"
      table_name = "${mysql_table_prefix}"
      repartition = "100"
      split = "${mysql_id}"
      user = "${mysql_user}"
      password = "${mysql_password}"
      columns = "${mysql_columns}"
      hivedbtbls = "${hive_db}.${hive_table}"
      where = "${mysql_where}"
      partitionKeys = "${hive_partition_keys}"
      partitionValues = "${hive_partition_values}"
      location="${hive_table_location}"
   }
}
```


### 3. org.interestinglab.waterdrop.input.MyJdbcByMod

用法：如果mysql索引分布不均匀可以选用这个插件。将ID取模平均分组

```java 
input {
org.interestinglab.waterdrop.input.MyJdbcByMod {
#mysql地址
url="jdbc:mysql://xxx.mysql.rds.aliyuncs.com/short_video?zeroDateTimeBehavior=convertToNull"
#想要同步的表
table="xxx"
#默认参数
table_name="waterdrop_log"
#将colmn分成多少份数据如果有特别大的数据倾斜不太好使
repartition="100"
#必填整型的自增列
column="id"
user="xxx"
password="xxx"
#默认参数
result_table_name="access_log"
}
}
```




