//import org.apache.spark.sql._
//import org.apache.spark.rdd.RDD
//
//object queries {
//
//  case class Packet(req_id: Long, time: Long, payload: String)
//  case class Server(ip: Int, incoming: Set[Packet], outgoing: Set[Packet])
//
//  var server: RDD[Server] = _
//  var SLA: Long = _
//
//  val q = server.map(s =>
//    (s.ip,
//      for (in_pkt <- s.incoming;
//           out_pkt <- s.outgoing;
//           if in_pkt.req_id == out_pkt.req_id &&
//              out_pkt.time - in_pkt.time > SLA &&
//              out_pkt.payload.startsWith("INVALID"))
//        yield in_pkt.payload)
//  )
//
//}
