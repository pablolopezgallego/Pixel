import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
implicit val formats = DefaultFormats
case class evar8(evar7:String)
case class adn(os:String,idTracker:String,ip:String,useragent:String,dispositivo:String,language:String,parametros:evar8,evar7:String)


val record = parse("{\"idTracker\":\"09013f4b164366c73d57f9f1e43c4a33\",\"ip\":\"::ffff:159.147.124.253\",\"url\":\"http://www.clinicalondres.es/tratamientos-tienes-40-anos-promociones-clinicalondres\",\"parametros\":{\"referer\":\"\",\"evar7\":\"hola\",\"evar39\":\"\",\"evar49\":\"\"},\"decay\":1513211004544,\"useragent\":\"Chrome\",\"os\":\"Windows\",\"dispositivo\":\"PC\",\"language\":\"es-ES\"}")
//val aux2 = render(record \\ "evar7")
//val aux = record.ip

val aux = (render(record))

val JString(aux2)= record \ "ip"

println(aux)



//aux.parametros.evar7

