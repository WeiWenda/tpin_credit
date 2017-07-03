package wwd.entity

/**
  * Created by Administrator on 2016/4/27.
  */
class WholeVertexAttr(var name: String, var nsrsbh: String, var ishuman: Boolean) extends Serializable {

    var community_id: Long = 0
    var xydj:String = ""
    var xyfz:Int = 0
    var wtbz:Boolean = false

    def toCsvString: String = {
        if (ishuman)
            s"$nsrsbh,person"
        else
            s"$nsrsbh,company"
    }

    override def toString = s"WholeVertexAttr($name, $nsrsbh, $ishuman)"
}

object WholeVertexAttr {
    def apply(name: String, nsrsbh: String, ishuman: Boolean):WholeVertexAttr = {
        new WholeVertexAttr(name, nsrsbh.replace(".0", ""), ishuman)

    }

    def combineNSRSBH(name1: String, name2: String): String = {
        var name = ""
        if (name1 != null) {
            // 拆分
            val name1s = name1.split(";")
            for (name1 <- name1s) {
                if (!name.contains(name1)) {
                    if (name != "") {
                        // 合并
                        name = name + ";" + name1
                    } else {
                        name = name1
                    }
                }
            }
        }
        if (name2 != null) {
            // 拆分
            val name2s = name2.split(";")
            for (name2 <- name2s) {
                if (!name.contains(name2)) {
                    if (name != "") {
                        // 合并
                        name = name + ";" + name2
                    } else {
                        name = name2
                    }
                }
            }
        }
        name
    }

    //annotation of david:尽可能判断为企业
    def combine(a: WholeVertexAttr, b: WholeVertexAttr): WholeVertexAttr = {
        WholeVertexAttr(combineNSRSBH(a.name,b.name), combineNSRSBH(a.nsrsbh, b.nsrsbh), a.ishuman && b.ishuman)
    }
}


class VertexType(var isTZF: Boolean, var isNSR: Boolean, var isFDDBR: Boolean) extends Serializable {
    def combine(vertexType: VertexType): VertexType = {
        val TypeInt = this.toInt | vertexType.toInt
        val toReturn = VertexType.fromInt(TypeInt)
        toReturn
    }

    def description: String = {
        var result = ""
        result += (if (isTZF) "投资方 " else "")
        result += (if (isNSR) "纳税人 " else "")
        result += (if (isFDDBR) "法定代表人 " else "")
        result.trim
    }

    override def toString = s"VertexType($description)"

    def toInt: Int = {
        var ret = 0
        if (isTZF) {
            ret |= 4
        }
        if (isFDDBR) {
            ret |= 2
        }
        if (isNSR) {
            ret |= 1
        }
        ret
    }

}

object VertexType {

    def fromInt(vertexTypeInt: Int): VertexType = {
        val toReturn = vertexTypeInt match {
            case 1 => VertexType.NSR_ONLY;
            case 2 => VertexType.FDDBR_ONLY;
            case 4 => VertexType.TZF_ONLY;
            case 5 => VertexType.NSR_AND_TZF;
            case 6 => VertexType.FDDBR_AND_TZF;
            case _ => VertexType.UNDEFINE
        }
        toReturn
    }

    val UNDEFINE = new VertexType(false, false, false)
    val NSR_ONLY = new VertexType(false, true, false)
    val TZF_ONLY = new VertexType(true, false, false)
    val FDDBR_ONLY = new VertexType(false, false, true)
    val NSR_AND_TZF = new VertexType(true, true, false)
    val FDDBR_AND_TZF = new VertexType(true, false, true)
}
