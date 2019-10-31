package big13

import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by Administrator on 2019/2/14.
  */
object WordCountStreamScala {
    case class WordWithCount(word : String, count : Long) ;
    def main(args: Array[String]): Unit = {
        import org.apache.flink.streaming.api.scala._
        //创建流执行环境对象
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        //创建socket文本流
        val ds1 = env.socketTextStream("192.168.72.101" , 8088 , '\n' )

        //压扁行
        val ds2 = ds1.flatMap(line => line.split(" "))

        //变换，标一成对
        val ds3 = ds2.map(w => WordWithCount(w , 1))

        //分组
        val ds4 = ds3.keyBy("word") ;


        //窗口操作
        val ds5 = ds4.timeWindow(Time.seconds(5) , Time.seconds(1))


        //统计总数
        val ds6 = ds5.sum("count")

        ds6.print().setParallelism(1)

        env.execute("Socket Window WordCount hello world")
    }
}
