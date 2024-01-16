# DataHub
## 1.架构流程图
## 2.详细说明
### 2.1 数据生产

数据生产，也就是不同的数据来源，其中有三个来源：

+ SJZX通过FTP传输的文件
+ 手动从后端上传的文件
+ Kafka来源的数据

### 2.2 数据区域

目前数据区域有三块,分别是

1. Reception(接收区域)：负责接收来自于SJZX上传的不符合约定路径文件，根据然后移动到对应的符合约定路径文件（缓存区域）
2. Cache（缓存区域）：通过脚本监控该区域的文件变动，根据对应的配置文件自动推送到kafka topic
3. Backup(备份区域)：串行推送完kafka后，添加.done的后缀，移动到备份区域

### 2.3 检查点

目前涉及两个检查点：

1. 检查点1 : 负责统计每个往kafka推送的文件名称，文件行数，推送时间，topic，ck库中对应topic的数据量，生成这些数据后(ods_flie_line)，推送到kafka

   >推送时间: date
   >
   >文件行数: cat JIAKZL_TNMS_ONU_TY_DAY_V2_20231219.csv.done | wc -l | awk '{print $1}'
   >
   >start_count=$(tar -O -xzf ${file_path}|wc -l | awk '{print $1}')
   >
   >topic: ${topic}
   >
   >文件名称: file_name=$(basename "${file_path}")
   >
   >ck库中对应topic的数据量:

2. 检查点2 : 负责按天统计每个topic、文件数量、库中该topic最新的时间

### 2.4 数据汇聚

所有数据统一汇聚到kafka，走flink入库流程

### 2.5 数据消费

Flink负责消费kafka中数据，将数据进行相应的逻辑处理，最后SINK到Clickhouse
