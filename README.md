# DataHub

DataHub是一个集中式数据管理和处理平台，用于高效地收集、处理和存储来自多个来源的数据。

## 1. 架构流程图

![架构图](https://telegraph-image-2ni.pages.dev/file/656dde774c3ed30b8a9f5.png)

## 2. 详细说明

### 2.1 数据生产

数据生产涉及多个来源：

- SJZX通过FTP传输的文件
- 通过后端手动上传的文件
- Kafka流式数据

### 2.2 数据区域

数据处理分为三个主要区域：

1. Reception（接收区域）：
   - 接收来自SJZX的非标准路径文件
   - 将文件移动到符合约定路径的缓存区域

2. Cache（缓存区域）：
   - 通过脚本监控文件变动
   - 根据配置文件自动将数据推送到指定Kafka topic

3. Backup（备份区域）：
   - 完成Kafka推送后，为文件添加`.done`后缀
   - 将处理完毕的文件移动到备份区域

### 2.3 检查点

系统包含两个关键检查点：

1. 检查点1：
   - 统计推送到Kafka的文件信息
   - 记录文件名称、行数、推送时间、topic及ClickHouse中对应topic的数据量
   - 生成`ods_file_line`数据并推送到Kafka

   统计方法：
   ```bash
   # 推送时间
   date
   
   # 文件行数
   cat JIAKZL_TNMS_ONU_TY_DAY_V2_20231219.csv.done | wc -l | awk '{print $1}'
   
   # 压缩文件行数
   start_count=$(tar -O -xzf ${file_path} | wc -l | awk '{print $1}')
   
   # Topic名称
   ${topic}
   
   # 文件名称
   file_name=$(basename "${file_path}")
   
   # ClickHouse中对应topic的数据量（待实现）
   ```

2. 检查点2：
   - 按天统计每个topic的文件数量
   - 记录ClickHouse中每个topic的最新数据时间

### 2.4 数据汇聚

所有数据统一汇聚到Kafka，为后续的Flink处理做准备。

### 2.5 数据消费

Flink负责从Kafka消费数据，主要职责包括：
- 消费Kafka中的数据
- 进行必要的逻辑处理
- 将处理后的数据写入ClickHouse存储

## 3. 技术栈

- 数据传输：FTP, Kafka
- 数据处理：Flink
- 数据存储：ClickHouse
- 脚本语言：Bash（用于文件处理和统计）

## 4. 配置和部署

- clickhouse集群搭建
- kafka集群搭建
- hadoop集群搭建

## 5. 监控和维护

- 后端对外暴露打点统计接口

## 6. 未来改进

- 实现ClickHouse数据量的实时统计
- 优化文件处理效率
- 增强数据质量检查机制
- 实现更详细的数据处理监控

