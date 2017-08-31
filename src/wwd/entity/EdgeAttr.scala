package wwd.entity

import java.text.DecimalFormat

/**
  * Created by weiwenda on 2017/3/15.
  */
class EdgeAttr extends Serializable {
    var tz_bl: Double = 0.0
    var jy_bl: Double = 0.0
    var kg_bl: Double = 0.0
    var il_bl: Double = 0.0


    override def toString(): String = {
        var toReturn = ""
        val formater = new DecimalFormat("#.###")
        if(il_bl > 0){
            toReturn += "互锁：+"+formater.format(il_bl)+"；"
        }
        if(tz_bl>0){
            toReturn += "投资："+formater.format(tz_bl)+"；"
        }
        if(kg_bl>0){
            toReturn += "控股："+formater.format(kg_bl)+"；"
        }
        if(jy_bl>0){
            toReturn += "交易："+formater.format(jy_bl)+"；"
        }
        return toReturn
    }

    override def hashCode(): Int = {
        val state = Seq()
        state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }
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
    var edgeAttr:EdgeAttr= null;
    override def toString = s"InfluenceEdgeAttr($bel, $pl, $src, $dst)"
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
