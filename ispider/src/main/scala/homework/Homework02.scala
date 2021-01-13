package homework

import java.util.Properties

import com.kkb.stream.common.util.jedis.PropertiesUtil
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
/**
 * @author Shi Lei
 * @create 2021-01-11
 */
object Homework02 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
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


    //todo: 3、构建kafka消费者
    val kafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]( topicName,
      new SimpleStringSchema(),
      properties)
    //指定从最新的数据开始消费
    kafkaConsumer.setStartFromEarliest()


    val sourceData: DataStream[String] = env.addSource(kafkaConsumer)

    sourceData.print()

    //todo:启动flink程序
    env.execute("homework02")

  }
}
