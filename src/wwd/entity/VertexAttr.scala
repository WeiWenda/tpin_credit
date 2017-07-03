package wwd.entity
/**
  * Created by weiwenda on 2017/3/15.
  */
class VertexAttr(var nsrdzdah: String, var fddbr: String) extends Serializable{
    var xydj:String = ""
    var xyfz:Int = 0
    var wtbz :Boolean = false

    override def toString = s"VertexAttr($xyfz, $nsrdzdah, $fddbr)"
}
object VertexAttr{
    def apply(nsrdzdah:String,fddbr:String):VertexAttr={
        new VertexAttr(nsrdzdah,fddbr)
    }
}
