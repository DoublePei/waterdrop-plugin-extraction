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

从airflow，gobblin，或者sqoop 转为waterdrop自研的优点与好处

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

未解决的点:
1.更改字段比较麻烦。
2.无法同步mysql的comment信息
