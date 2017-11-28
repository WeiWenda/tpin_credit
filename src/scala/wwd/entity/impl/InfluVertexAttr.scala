package wwd.entity.impl

import wwd.entity.VertexAttr

/**
  * Created by weiwenda on 2017/3/15.
  */
class InfluVertexAttr(var nsrdzdah: String, var fddbr: String) extends VertexAttr with Serializable{
    var xydj:String = ""
    var xyfz:Int = 0
    var wtbz :Boolean = false

    override def toString = s"VertexAttr($xyfz, $nsrdzdah, $fddbr)"
}
object InfluVertexAttr{
    def apply(nsrdzdah:String,fddbr:String):InfluVertexAttr={
        new InfluVertexAttr(nsrdzdah,fddbr)
    }
}
