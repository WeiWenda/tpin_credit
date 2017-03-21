package wwd.entity

/**
  * Created by weiwenda on 2017/3/15.
  */
class EdgeAttr extends Serializable {
    var tz_bl: Double = 0.0
    var jy_bl: Double = 0.0
    var kg_bl: Double = 0.0
}

object EdgeAttr {
    def apply() = {
        new EdgeAttr()
    }

    def combine(a: EdgeAttr, b: EdgeAttr): EdgeAttr = {
        if (b.kg_bl > 0)
            a.kg_bl += b.kg_bl
        else if (b.jy_bl > 0)
            a.jy_bl += b.jy_bl
        else
            a.tz_bl += b.tz_bl
        a
    }
}
class InfluenceEdgeAttr(val bel:Double,val pl:Double) extends Serializable {
}

object InfluenceEdgeAttr {
    def apply(bel:Double,pl:Double) = {
        new InfluenceEdgeAttr(bel,pl)
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
