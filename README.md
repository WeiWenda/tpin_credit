# 1.构建交易网络
V={股东列表，自然人投资方列表，法定代表人}
E={交易，投资、控股}
# 2.计算影响力网络
## 2.1 计算每条边的Bel,Pl
### Bel表示影响力下限，表示不少于多大程序的影响，Pl表示影响力上限，表示不多于多大程度的影响

 - 将自然人的投资比例、控股比例规范到0-1
 - controllerInterSect = 自然人亲密度，[0,1]
 - Bel =  Seq(controllerInterSect, tz_bl, kg_bl, jy_bl).filter(_>0).min
 - Pl = Seq(controllerInterSect, tz_bl, kg_bl, jy_bl).max

## 2.2 按Bel选择TopN的邻居
## 2.3 获取路径 
Seq[Seq[(VertexId, Double, Double)]]，两个Double分别是Bel和Pl
## 2.4 计算每条路径上的影响值并融合
## 2.5 结合自然人亲密度得到影响力网络

 - 过滤影响力小于0.01的影响力边

# 3.与信用评分融合
## 3.1 对已有评分的企业先行修正

    def AggregateMessage(xyfz: Int, listMessage: scala.Seq[( Int, Double)]): Int = {
        val totalWeight = listMessage.filter(_._1<xyfz).map(_._2).sum
        val Sortedlist = listMessage.sortBy(_._2)(Ordering[Double].reverse)
        var i = 0
        var res = 0D
        while(i< Sortedlist.size){
            val (cur_fx,weight) = Sortedlist(i)
            if(cur_fx < xyfz){
                res += (xyfz-cur_fx) *weight/totalWeight
            }
            i+=1
        }
        (xyfz - res).toInt
    }
## 3.2 对未评分的企业进行修正
加权平均
# 4.实验部分
## 4.1 问题纳税人定义 SHANNXI.WWD_INFLUENCE_RESULT

> 2014年至2015年稽查表中
> WFWZLX_DM不为空的企业,wtbz = 'true' 
> WFWZLX_DM为空的企业,wtbz ='false' 
> 未出现在2014年至2015年稽查表的企业,wtbz = 'no'