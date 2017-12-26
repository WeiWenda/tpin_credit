package wwd.entity.impl

import java.text.DecimalFormat

import wwd.entity.EdgeAttr

/**
  * Created by weiwenda on 2017/3/15.
  */
class InfluEdgeAttr extends EdgeAttr with Serializable {
    var tz_bl: Double = 0.0
    var jy_bl: Double = 0.0
    var kg_bl: Double = 0.0
    var il_bl: Double = 0.0


    override def toString(): String =  {
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

object InfluEdgeAttr{
    def apply() = {
        new InfluEdgeAttr()
    }

    def combine(a: InfluEdgeAttr, b: InfluEdgeAttr): InfluEdgeAttr = {
        a.kg_bl += b.kg_bl
        a.jy_bl += b.jy_bl
        a.tz_bl += b.tz_bl
        a.il_bl += b.il_bl
        a
    }
}
