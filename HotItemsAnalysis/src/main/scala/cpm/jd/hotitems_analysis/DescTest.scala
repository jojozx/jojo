package cpm.jd.hotitems_analysis
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.util.Map;

object DescTest {
  def main(args: Array[String]): Unit = {
    val Str="2020-04-10 11:31:03.730 INFO [qtp1833848849-34]c.j.s.r.c.GoodsSearchController.|{\"f3b52135-e0e4-4a70-a0be-e49172d2eae9:EsQuery\":{\"size\":999,\"query\":{\"bool\":{\"adjust_pure_negative\":true,\"must\":[{\"term\":{\"isDelete\":{\"boost\":1.0,\"value\":0}}},{\"terms\":{\"newGoodsSn.raw\":[\"1030200121\"],\"boost\":1.0}}],\"boost\":1.0}},\"from\":0,\"_source\":{\"excludes\":[\"goodsDesc\"],\"includes\":[]},\"sort\":[{\"id\":{\"order\":\"desc\"}}],\"timeout\":\"60s\"},\"_traceId\":\"1586489463723WFAP\",\"consumeTime\":7,\"f3b52135-e0e4-4a70-a0be-e49172d2eae9:EsTake\":\"1\",\"f3b52135-e0e4-4a70-a0be-e49172d2eae9:EsIndexName\":\"[\\\"sit_stall_goods\\\"]\",\"meg\":\"成功\",\"status\":200}"
    val spl=Str.split(" ")
      if(spl(2)=="INFO"){
        val splS=spl(3).split("\\|")
        val mapTypes :Map[String ,Object]= JSON.parseObject( splS(1))
        import scala.collection.JavaConversions._
        for (obj <- mapTypes.keySet) {
          System.out.println("key为：" + obj + "----值为：" + mapTypes.get(obj))
        }
//        for (i<-splS.indices){
//                 println(splS(i))
//              }
      }
//    for (i<-spl.indices){
//       println(spl(i))
//    }
  }

}
