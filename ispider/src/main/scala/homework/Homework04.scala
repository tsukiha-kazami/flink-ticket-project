package homework

import java.util.{Date, Properties}

import com.kkb.stream.common.bean.ProcessedData
import com.kkb.stream.common.util.jedis.PropertiesUtil
import com.kkb.stream.dataprocess.businessProcess.QueryDataPackage
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

/**
 * @author Shi Lei
 * @create 2021-01-13
 */
object Homework04 {


  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    //获取kafka集群地址
    val bootstrapServers: String = PropertiesUtil.getStringByKey("bootstrap.servers", "kafkaConfig.properties")
    //获取topic名称
    val topicName: String = PropertiesUtil.getStringByKey("target.query.topic", "kafkaConfig.properties")
    //获取消费者组id
    val groupID: String = PropertiesUtil.getStringByKey("rule.group.id", "kafkaConfig.properties")

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", bootstrapServers)
    properties.setProperty("topic.name", topicName)
    properties.setProperty("group.id", groupID)

    val kafkaConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](topicName,
      new SimpleStringSchema(),
      properties)
    //指定从最新的数据开始消费
    kafkaConsumer.setStartFromEarliest()


    val sourceData: DataStream[String] = env.addSource(kafkaConsumer)
    //sourceData.print()

    val processedDataStream: DataStream[ProcessedData] = QueryDataPackage.queryDataLoadAndPackage(sourceData)


    val result: DataStream[(String, String, String, Int)] = statisticMethodAndIp(processedDataStream)
    result.print("homework04")

    env.execute("homework04")
  }

  def statisticMethodAndIp(ds: DataStream[ProcessedData]): DataStream[(String, String, String, Int)] = {
    val value: DataStream[(String, String)] = ds.map(x => {
      val method: String = x.requestMethod
      val ip: String = x.remoteAddr
      //      print("ip: " + ip + " method: " + method)
      (ip, method)
    })
    val result: DataStream[(String, String, String, Int)] = value.keyBy(0, 1)
      .timeWindow(Time.seconds(10))
      .process(new MyProcessWindowFunction)
    result
  }


}

class MyProcessWindowFunction extends ProcessWindowFunction[(String, String), (String, String, String, Int), Tuple, TimeWindow] {
  override def process(key: Tuple, context: Context, elements: Iterable[(String, String)], out: Collector[(String, String, String, Int)]): Unit = {
    val ele: List[(String, String)] = elements.toList
    val date = new Date().toString
    val ip: String = key.getField[String](0)
    val method: String = key.getField[String](1)
    val count: Int = ele.size
    println(date + " ip: " + ip + " method: " + method + " count: " + count)
    out.collect((date, ip, method, count))

  }
}