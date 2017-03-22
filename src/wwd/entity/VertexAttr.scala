package wwd.entity
/**
  * Created by weiwenda on 2017/3/15.
  */
class VertexAttr(var nsrdzdah: String, var fddbr: String) extends Serializable{
    var gd_list: Seq[(String,Double)] = null
    var zrrtz_list:Seq[(String,Double)] = null
    var xydj:String = null
    var xyfz:Int = 0

    override def toString = s"VertexAttr($gd_list, $zrrtz_list, $xyfz, $nsrdzdah, $fddbr)"
}
object VertexAttr{
    def apply(nsrdzdah:String,fddbr:String):VertexAttr={
        new VertexAttr(nsrdzdah,fddbr)
    }
}
