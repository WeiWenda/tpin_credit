package wwd.entity

/**
  * Created by weiwenda on 2017/3/15.
  */
class EdgeAttr extends Serializable {
    var tz_bl: Double = 0.0
    var jy_bl: Double = 0.0
    var kg_bl: Double = 0.0
    var il_bl: Double = 0.0

    override def toString = s"EdgeAttr($tz_bl, $jy_bl, $kg_bl,$il_bl)"
}

object EdgeAttr {
    def apply() = {
        new EdgeAttr()
    }

    def combine(a: EdgeAttr, b: EdgeAttr): EdgeAttr = {
        a.kg_bl += b.kg_bl
        a.jy_bl += b.jy_bl
        a.tz_bl += b.tz_bl
        a.il_bl += b.il_bl
        a
    }
}
class InfluenceEdgeAttr(val bel:Double,val pl:Double,val src:String,val dst:String) extends Serializable {

    override def toString = s"InfluenceEdgeAttr($bel, $pl,$src,$dst)"
}

object InfluenceEdgeAttr {
    def apply(bel:Double,pl:Double,src:String,dst:String) = {
        new InfluenceEdgeAttr(bel,pl,src,dst)
    }
}
//class InfluencePathAttr(var pTrust: Double,var uncertainty: Double) extends Serializable {
//
//}
//
//object InfluencePathAttr {
//    def apply( pTrust: Double,uncertainty: Double) = {
//        new InfluencePathAttr(pTrust,uncertainty)
//    }
//}
