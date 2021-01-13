package homework.mysql

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util

import com.kkb.stream.common.util.database.C3p0Util
import com.kkb.stream.common.util.jedis.JedisConnectionUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import redis.clients.jedis.Jedis

import scala.collection.mutable.ArrayBuffer

/**
 * @author Shi Lei
 * @create 2021-01-12
 */
class MysqlSourceContext extends RichSourceFunction[util.HashMap[String, Any]] {

  var connection: Connection = _
  //定义过滤规则查询的对象
  var ps1: PreparedStatement = _
  var rs1: ResultSet = _

  var map: util.HashMap[String, Any] = _

  //标识
  var isRunning = true

  override def open(parameters: Configuration): Unit = {
    //获取数据库链接
    connection = C3p0Util.getConnection
    val filterRuleSql = "select id,value from nh_filter_rule"
    ps1 = connection.prepareStatement(filterRuleSql)
    rs1 = ps1.executeQuery()

    //定义map封装查询结果
    map = new util.HashMap[String, Any]()
    map = queryFilterRuleToMap(rs1, map)

  }

  override def run(ctx: SourceContext[util.HashMap[String, Any]]): Unit = {
    while (isRunning && !Thread.interrupted()) {
      val jedis: Jedis = JedisConnectionUtil.getSimpleJedis
      //从redis中查询是否更新规则的标识
      //过滤规则标识
      val filterIsUpdate = jedis.get("filterChangeFlag").toBoolean

      if (filterIsUpdate) {

        println("---------[filterChangeFlag]规则发生变化----------")

        //移除map中的key
        map.remove("filterRule")

        //查询mysql中更新的规则数据
        rs1 = ps1.executeQuery()

        //封装更新的规则到map中
        map=queryFilterRuleToMap(rs1,map)

        //修改redis中的更新规则标识
        jedis.set("filterChangeFlag", "false")

      }//end if
      //输出
      ctx.collect(map)
//      println(map)

      //每隔5s检测redis中的key
      Thread.sleep(5000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }


  def queryFilterRuleToMap(rs: ResultSet, map: util.HashMap[String, Any]): util.HashMap[String, Any] = {

    val filterList = ArrayBuffer[String]()
    //遍历过滤规则数据
    while (rs.next()) {
      val value: String = rs.getString("value")
      //添加过滤规则到list集合中
      filterList.+=(value)
      //添加key-value到map中，这里用得到固定的key，方便后续删除
      map.put("filterRule", filterList)
    }
    map
  }
}
