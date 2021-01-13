package homework

import java.util
import java.util.Properties

import com.kkb.stream.common.util.jedis.PropertiesUtil
import homework.broadcast.MyRuleBroadcastProcessFunction
import homework.mysql.MysqlSourceContext
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{BroadcastConnectedStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
 * @author Shi Lei
 * @create 2021-01-12
 */
object Homework03 {
  def main(args: Array[String]): Unit = {



    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    //开启checkpoint
    //    checkpoint配置
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointTimeout(5000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //设置statebackend
    env.setStateBackend(new FsStateBackend("hdfs://node01:8020/flink_kafka/checkpoints",true))

    //获取kafka集群地址
    val bootstrapServers: String = PropertiesUtil.getStringByKey("bootstrap.servers","kafkaConfig.properties")
    //获取topic名称
    val topicName: String = PropertiesUtil.getStringByKey("source.nginx.topic","kafkaConfig.properties")
    //获取消费者组id
    val groupID: String = PropertiesUtil.getStringByKey("group.id","kafkaConfig.properties")
    //flink自动检测topic新增分区
    val partitionDiscovery: String = PropertiesUtil.getStringByKey("flink.partition-discovery.interval-millis","kafkaConfig.properties")

    //构建配置对象
    val properties = new Properties()
    properties.setProperty("bootstrap.servers",bootstrapServers)
    properties.setProperty("topic.name",topicName)
    properties.setProperty("group.id",groupID)
    properties.setProperty("flink.partition-discovery.interval-millis",partitionDiscovery)


    //构建kafka消费者
    val kafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]( topicName,
      new SimpleStringSchema(),
      properties)
    //指定从最新的数据开始消费
//    kafkaConsumer.setStartFromEarliest()
    kafkaConsumer.setStartFromGroupOffsets()


    val sourceData: DataStream[String] = env.addSource(kafkaConsumer)
    sourceData.print()
    //读取mysql中的数据
    val ruleStream: DataStream[util.HashMap[String, Any]] = env.addSource(new MysqlSourceContext)
//    ruleStream.print()

    //創建广播流
    val mapStateDesc = new MapStateDescriptor("air_rule",classOf[Void],classOf[Map[String,Any]])
    //将规则数据流广播，形成广播流
    val ruleBroadcastStream: BroadcastStream[util.HashMap[String, Any]] = ruleStream.broadcast(mapStateDesc)

    //事件流和规则流合并
    val ruleBroadcastConnectedStream: BroadcastConnectedStream[String, util.HashMap[String, Any]] = sourceData.connect(ruleBroadcastStream)
//根据配置(规则)处理事件
    val structureDataStream: DataStream[String] = ruleBroadcastConnectedStream.process(new MyRuleBroadcastProcessFunction)
    structureDataStream.print("homework03")


    env.execute("homework03")

  }
}
