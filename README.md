# OctopusDB
Separating Keys from Values. Distributed Database System. Support Graph Query. 

# 常见LSM的写入流程
![img.png](doc/lsm/write-normal-lsm.png)
1. 接收到用户写入请求后，首先写入`wal`和`memtable`：`wal`用做预写日志，持久化，`memtable`是kv存储的内存结构，通常使用跳表实现
2. 后台主要有两个任务：将`memtable`刷到磁盘，以及对`sst`文件做合并
   1. 内存中同一时刻只有一个活跃的`memtable`接收写入请求，其他都为`immemtable`。当内存中的`memtable`大小达到阈值之后，会转变为`sst`并刷到磁盘上
   2. `sst`就是有序存储的数据文件，`SST` 的末尾是若干个索引块，记录每个数据块开头的 key，以便快速定位 key 的位置。`SST` 中可能含有 `Bloom Filter` 等结构加速查询。
3. 第0层包含多个`SST`文件，每个文件包含的key范围可能重复，从L1层开始，同一层的文件内key不会重复，后台线程会对超出容量的`SST`文件做合并

# What's SSTable
> SET(k,v)-->内存表大小达到阈值-->flush到磁盘sst文件
1. 从读写两个角度分析sst的使用场景：
   1. 写入kv导致内存大小达到阈值，需要flush，写入sst文件
   2. 初始化db，需要加载sst文件，构建内存索引
2. 需要考虑的问题：
   1. 如何序列化：从内存到磁盘，序列化是不可避免的问题
   2. 通用的序列化思路：`meta ｜ index ｜ data`
   3. 如何高效的读写？使用`mmap`技术，磁盘--用户空间直接映射，用户操作内存`[]byte`，系统负责异步写磁盘
   
# Manifest
1. 作用：存储`sst`文件层级信息的元数据文件，因此在flush（新建sst文件），merge（sst文件合并时），都需要对manifest进行更新
2. 单独使用此类型文件来记录`sst`的元信息，也是为了加快数据库的恢复

# 布隆过滤器
1. 作用：判断key是否存在某个`sst`文件中，避免每次都将文件读取到内存中处理
2. false positive: 为1不一定存在，为0一定不存在