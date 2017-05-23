package wwd.entity

/**
  * Created by Administrator on 2016/4/27.
  */
class WholeEdgeAttr(var w_control: Double,
               var w_tz: Double,
               var w_gd: Double,
               var w_trade: Double
              ) extends Serializable {

    def toCsvString: String = {
        var result = ""
        if (w_control != 0.0) result = "法定代表人"
        if (w_tz != 0.0) result = "投资"
        if (w_gd != 0.0) result = "股东"
        if (w_trade != 0.0) result = "交易"
        if (is_IL != false) result = "互锁"
        result
    }

    //annotation of david:社团id
    var community_id: Long = 0L
    //annotation of david:交易金额、投资金额、税率、税额
    var trade_je: Double = 0.0
    var tz_je: Double = 0.0
    var taxrate: Double = 0.0
    var se: Double = 0.0


    //annotation of david:互锁边标识位
    var is_IL: Boolean = false;
    lazy val w_IL = if (is_IL) 1.0 else 0.0

    //annotation of david:前件路径
    def isAntecedent(weight: Double=0.0, justGD :Boolean = false ): Boolean = {
        if (this.is_IL) return false
        //annotation of david:严格的，当weight取值为0.0时，允许等号将导致交易边被 count in
        if(!justGD)
            (this.w_control > weight || this.w_tz > weight || this.w_gd > weight)
        else
            (this.w_gd > weight)
    }

    def isTrade(): Boolean = {
        this.w_trade != 0.0
    }

    override def toString = s"EdgeAttr(控制:$w_control, 投资:$w_tz, 交易:$w_trade, 互锁:$w_IL)"
}

object WholeEdgeAttr {
    def fromString(s: String): WholeEdgeAttr = {
        s match {
            case "交易" => WholeEdgeAttr(w_trade = 1.0);
            case "控制" => WholeEdgeAttr(w_control = 1.0);
            case "投资" => WholeEdgeAttr(w_tz = 1.0);
        }
    }

    def combine(a: WholeEdgeAttr, b: WholeEdgeAttr) = {
        val toReturn = new WholeEdgeAttr(a.w_control + b.w_control, a.w_tz + b.w_tz,a.w_gd+b.w_gd, a.w_trade + b.w_trade)
        toReturn.trade_je = a.trade_je + b.trade_je
        toReturn.tz_je = a.tz_je + b.tz_je
        toReturn.se = a.se + b.se
        toReturn
    }

    def apply(w_control: Double = 0.0, w_tz: Double = 0.0,w_gd:Double=0.0, w_trade: Double = 0.0) = {
        val lw_control = if (w_control > 1.0) 1.0 else w_control
        val lw_tz = if (w_tz > 1.0) 1.0 else w_tz
        val lw_trade = if (w_trade > 1.0) 1.0 else w_trade
        val lw_gd = if (w_gd > 1.0) 1.0 else w_gd
        new WholeEdgeAttr(lw_control, lw_tz,lw_gd, lw_trade)
    }
}