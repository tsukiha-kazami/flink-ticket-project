package homework.broadcast

import java.util

import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

/**
 * @author Shi Lei
 * @create 2021-01-12
 */
class MyRuleBroadcastProcessFunction extends BroadcastProcessFunction[String, util.HashMap[String, Any], String] {

  val mapStateDesc = new MapStateDescriptor("air_rule", classOf[Void], classOf[util.HashMap[String, Any]])

  /**
   * @param map 广播流的数据
   * @param ctx 上下文
   * @param out 输出数据
   */
  override def processBroadcastElement(map: util.HashMap[String, Any], ctx: BroadcastProcessFunction[String, util.HashMap[String, Any], String]#Context, out: Collector[String]): Unit = {
    //获取广播状态
    val filterRuleBroadCastState: BroadcastState[Void, util.HashMap[String, Any]] = ctx.getBroadcastState(mapStateDesc)

    //清理状态
    filterRuleBroadCastState.clear()

    //更新状态
    filterRuleBroadCastState.put(null, map)

  }

  override def processElement(message: String, ctx: BroadcastProcessFunction[String, util.HashMap[String, Any], String]#ReadOnlyContext, out: Collector[String]): Unit = {
    //todo: 1、获取广播状态
    val ruleBroadCastState: ReadOnlyBroadcastState[Void, util.HashMap[String, Any]] = ctx.getBroadcastState(mapStateDesc)
    //todo：这个hashMap其本质就是最开始封装的mysql表中的规则数据
    val ruleDataMap: util.HashMap[String, Any] = ruleBroadCastState.get(null)

    //todo: 2、数据的过滤处理
    //匹配标记,默认为 true 表示所有数据都保留
    var save = true

    //对数据进行切分
    val split: Array[String] = message.split("#CS#")

    //取出 request 字段(request 数据为角标为 1 的数据)
    //todo: 它就是每一条数据请求的url地址
    val request = if (split.length > 1) split(1) else ""
    //    println(request)
    //获取map中过滤规则的key的value,转换为list集合
    val filterRuleIterator = ruleDataMap.get("filterRule").asInstanceOf[ArrayBuffer[String]].toIterator

    while (filterRuleIterator.hasNext && save) {
      if (request.matches(filterRuleIterator.next())) {
        //匹配到了规则，直接丢弃
        save = false
      }
    }


    out.collect(request)
  }


}
