package big13

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
  * 使用flink进行bath计算
  */
object WordCountBatchScala {
    def main(args: Array[String]): Unit = {
        //显式导入隐式转换
        import org.apache.flink.streaming.api.scala._
        val benv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

        //读取文件
        val ds1:DataSet[String] = benv.readTextFile("D:\\user\\hive\\warehouse\\dm_dmp.db\\dm_keyword_19\\dt=2019-03-26")

        ds1.print()
        //压扁
        ds1.mapPartition(par=>{
            par.map(row=>{
                row.split("\t")
            })
        })
        ds1.map(row=>{
            row.split("\t")
        })
        val ds2 = ds1.flatMap(line => line.split("\t"))
        val ds3 = ds2.map(w =>(w , 1))
        val ds4 = ds3.groupBy("_1")
        ds4.sum("_2").print()
    }

}
