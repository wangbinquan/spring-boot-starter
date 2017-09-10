# spring-boot-starter

Spring auto configure for<p/>
1.Spark<p/>
***spark.master*** and ***spark.app.name*** is required in yml or properities file<p/>
2.Hadoop<p/>
If you set the ***HDOOP_PATH***, it will find hdfs properities in ***HDOOP_PATH/etc/hadoop***, or you have to set the ***hadoop.uri*** in yml or properities file<p/>
3.Hbase<p/>
If you set the ***HBASE_PATH***, it will find hbase properities in ***HBASE_PATH/conf***, or you have to set the ***hbase.zookeeper.quorum*** in yml or properities file<p/>
