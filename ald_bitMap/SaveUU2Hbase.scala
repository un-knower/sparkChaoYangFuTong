package aldwxstat.bitmap

import java.text.SimpleDateFormat

import aldwxutils.ArgsTool
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong
import org.apache.curator.retry.{ExponentialBackoffRetry, RetryNTimes}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SaveUU2Hbase {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
//      .master("local[3]")
      .getOrCreate()

    ArgsTool.analysisArgs(args)
//    val df: DataFrame = sparkSession.read.parquet("D:\\Program\\data\\m\\521small.parquet")
    val df: DataFrame = ArgsTool.getSpecifyTencentDateDF(args, sparkSession, Array(0))

    val zkUrl="10.0.100.13,10.0.100.8,10.0.100.14"
//    val tableName="ald_user_id_map"
    val tableName="test_user_id_map2"

    //把hbase中没有的uuid信息插入到hbase
    insertUuid2Hbase(sparkSession,df,ArgsTool.day,tableName,zkUrl)

    sparkSession.close()
  }
  /**
    * 把hbase中没有的uuid信息插入到hbase
    * @param sparkSession
    */
  def insertUuid2Hbase(sparkSession: SparkSession, df: DataFrame, day: String,tableName: String,zkUrl:String,zkPort:String="2181"): Unit = {

    //如果表不存在则创建
    createTableOrDoNothing(tableName, zkUrl)

    val uuidRDD = df.filter("ev='app'").select(
      df("uu"), df("ak"), df("ifo")
    ).rdd.map(row => ((row.getString(0), row.getString(1)), row.getString(2)))
      .reduceByKey((ifo1, ifo2) => {
        //去重聚合
        if ("true".equals(ifo1) || "true".equals(ifo2)) day else ""
      }).map(line => {
      val ifo = if ("true".equals(line._2)) day else ""
      (line._1._1, line._1._2, ifo)//(uu,ak,ifo)
    })
      .mapPartitions(itr => {
        val buf = new collection.mutable.ArrayBuffer[(String, String, String,Int)]()
        //获取hbase连接
        val  connection = getHbaseConn(tableName, zkUrl, zkPort)
        //建立表的连接
        val table = connection.getTable(TableName.valueOf(tableName))
        val gets = new java.util.ArrayList[Get]
        val list = itr.toList
        //列族
        val cf1 = Bytes.toBytes("user_id")
        //列限定符
        val qf = Bytes.toBytes("sequence_id")
        list.foreach { case (uu, ak, ifo) => {
          //行键
          val row1 = Bytes.toBytes(ak + uu)
          val get1 = new Get(row1)
          get1.addColumn(cf1, qf)
          gets.add(get1)
        }
        }

        //     查看结果集里是否有ak,uu,没有则放入buf
        val results: Array[Result] = table.get(gets)
        var num=0
        for(i<- 0 until results.length){
          if(Bytes.toString(results(i).getRow) == null){
            val uu=list(i)._1
            val ak=list(i)._2
            val ifo=list(i)._3
            num = num+1
            buf.append((uu, ak, ifo,num))
          }
        }

        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm")
        val update = sdf.format(System.currentTimeMillis())
        //        zk保存增量
        val PATH = "/test/counter"
        val countUrl="10.0.100.13:2181,10.0.100.8:2181,10.0.100.14:2181"
        val client = CuratorFrameworkFactory.newClient(countUrl, new ExponentialBackoffRetry(1000, 30))
        client.start()
        //计算增量值
        val count = new DistributedAtomicLong(client, PATH, new RetryNTimes(10, 10))
        val add = count.add(buf.length.toLong)
        val preValue = add.preValue()

        val rs = buf.map {
          case (user_id, ak, ifo, num) => {
            val sequence_id = num + preValue
            val put = new Put(Bytes.toBytes(s"$ak$user_id"))
            put.addColumn(Bytes.toBytes("ak"), Bytes.toBytes("ak"), Bytes.toBytes(ak))
            put.addColumn(Bytes.toBytes("user_id"), Bytes.toBytes("user_id"), Bytes.toBytes(user_id))
            put.addColumn(Bytes.toBytes("user_id"), Bytes.toBytes("sequence_id"), Bytes.toBytes(sequence_id.toInt))
            put.addColumn(Bytes.toBytes("ifo"), Bytes.toBytes("day"), Bytes.toBytes(ifo))
            put.addColumn(Bytes.toBytes("time"), Bytes.toBytes("update"), Bytes.toBytes(update))
            //            myTable.put(put)
            (new ImmutableBytesWritable, put)
          }
        }

        //关闭连接
        client.close()
        connection.close()
        rs.iterator
      })
    /**
      * .map { case ((user_id, ak, ifo), index) => {
        val sequence_id = count + index//
        val put = new Put(Bytes.toBytes(s"$ak$user_id"))
        put.addColumn(Bytes.toBytes("ak"), Bytes.toBytes("ak"), Bytes.toBytes(ak))
        put.addColumn(Bytes.toBytes("user_id"), Bytes.toBytes("user_id"), Bytes.toBytes(user_id))
        put.addColumn(Bytes.toBytes("user_id"), Bytes.toBytes("sequence_id"), Bytes.toBytes(sequence_id.toInt))
        put.addColumn(Bytes.toBytes("ifo"), Bytes.toBytes("day"), Bytes.toBytes(ifo))
        put.addColumn(Bytes.toBytes("time"), Bytes.toBytes("update"), Bytes.toBytes(update))
        (new ImmutableBytesWritable, put)
      }
      */
    val jobConf = new JobConf()
    jobConf.set("hbase.zookeeper.quorum", zkUrl)
    //    jobConf.set("zookeeper.znode.parent", "/hbase")
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    uuidRDD.saveAsHadoopDataset(jobConf)

  }


  //获取hbase连接
  private def getHbaseConn(tableName: String, zkUrl: String, zkPort: String) = {
    val configuration = HBaseConfiguration.create
    //设置zooKeeper集群地址
    configuration.set("hbase.zookeeper.quorum", zkUrl)
    //设置zookeeper连接端口，默认2181
    configuration.set("hbase.zookeeper.property.clientPort", zkPort)
    val connection = ConnectionFactory.createConnection(configuration)
    connection
  }

  // 如果表不存在则创建表
  private def createTableOrDoNothing(tableName: String, zkUrl: String) = {
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum", zkUrl)
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    // 如果表不存在则创建表
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
      tableDesc.addFamily(new HColumnDescriptor("user_id"))
      tableDesc.addFamily(new HColumnDescriptor("ak"))
      tableDesc.addFamily(new HColumnDescriptor("ifo"))
      tableDesc.addFamily(new HColumnDescriptor("time"))
      admin.createTable(tableDesc)
    }
  }


}
