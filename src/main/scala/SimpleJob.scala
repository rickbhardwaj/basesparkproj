package basesparkproj

import spark.SparkContext
import SparkContext._

import com.esotericsoftware.kryo.Kryo

class MyRegistrator extends spark.KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[SimpleJob])
  }
}

class SimpleJob ( 
  val ts : Int,
  val t : Byte,
  val src : String,
  val dst : String,
  val domain : String,
  val qtype : Byte,
  val rcode : Byte
)

object SimpleJob {

  def main(args: Array[String]) {
    System.setProperty("spark.serializer", "spark.KryoSerializer")
    System.setProperty("spark.kryo.registrator", "chris.MyRegistrator")

    val sc = new SparkContext("spark://sec-store1.cs.berkeley.edu:7077", "SimpleJob", 
      "/opt/hadoop/spark", List("target/scala-2.9.3/simple-project_2.9.3-1.0.jar") ++ JARLIST) 

    val logFile = "hdfs://sec-store1:54310/user/hadoop/dns/raw/2012/03/raw_processed.20120301.0000.*.0.gz" 

    val textfile = sc.textFile(logFile, 2)
    val decoded = textfile.flatMap(m => {
      try {
        val tmp = m.split(",").slice(0,7)
        Some(new SimpleJob(tmp(0).toInt, tmp(1).toByte, tmp(2), tmp(3), tmp(4), tmp(5).toByte, tmp(6).toByte))
      } catch {
        case _ => None
      }
    }).filter(r => r.qtype == 1 && (r.rcode == 3 || r.rcode == 0))
    .map(r => (r.src, r)).groupByKey(25) 
    .map(resolver => resolver._2.groupBy(_.ts % 60))
    .persist(spark.storage.StorageLevel.MEMORY_ONLY_SER)

    println(decoded.count())

  }

  val JARLIST = List(
    "lib_managed/jars/com.google.protobuf/protobuf-java/protobuf-java-2.4.1.jar",
    "lib_managed/jars/org.objenesis/objenesis/objenesis-1.2.jar",
    "lib_managed/jars/org.mortbay.jetty/jsp-2.1/jsp-2.1-6.1.14.jar",
    "lib_managed/jars/org.mortbay.jetty/servlet-api-2.5/servlet-api-2.5-6.1.14.jar",
    "lib_managed/jars/org.mortbay.jetty/jsp-api-2.1/jsp-api-2.1-6.1.14.jar",
    "lib_managed/jars/org.mortbay.jetty/jetty/jetty-6.1.26.jar",
    "lib_managed/jars/org.mortbay.jetty/servlet-api/servlet-api-2.5-20081211.jar",
    "lib_managed/jars/org.mortbay.jetty/jetty-util/jetty-util-6.1.26.jar",
    "lib_managed/jars/javax.servlet/servlet-api/servlet-api-2.5.jar",
    "lib_managed/jars/net.databinder/dispatch-json_2.9.1/dispatch-json_2.9.1-0.8.5.jar",
    "lib_managed/jars/it.unimi.dsi/fastutil/fastutil-6.4.4.jar",
    "lib_managed/jars/commons-httpclient/commons-httpclient/commons-httpclient-3.0.1.jar",
    "lib_managed/jars/asm/asm-all/asm-all-3.3.1.jar",
    "lib_managed/jars/org.parboiled/parboiled-scala/parboiled-scala-1.0.2.jar",
    "lib_managed/jars/org.parboiled/parboiled-core/parboiled-core-1.0.2.jar",
    "lib_managed/jars/concurrent/concurrent/concurrent-1.3.4.jar",
    "lib_managed/jars/commons-collections/commons-collections/commons-collections-3.2.1.jar",
    "lib_managed/jars/commons-configuration/commons-configuration/commons-configuration-1.6.jar",
    "lib_managed/jars/org.apache.hadoop/hadoop-core/hadoop-core-1.0.4.jar",
    "lib_managed/jars/org.codehaus.jackson/jackson-core-asl/jackson-core-asl-1.0.1.jar",
    "lib_managed/jars/org.codehaus.jackson/jackson-mapper-asl/jackson-mapper-asl-1.0.1.jar",
    "lib_managed/jars/oro/oro/oro-2.0.8.jar",
    "lib_managed/jars/com.google.code.findbugs/jsr305/jsr305-1.3.9.jar",
    "lib_managed/jars/org.jvnet/mimepull/mimepull-1.6.jar",
    "lib_managed/jars/voldemort.store.compress/h2-lzf/h2-lzf-1.0.jar",
    "lib_managed/jars/com.github.scala-incubator.io/scala-io-core_2.9.2/scala-io-core_2.9.2-0.4.1.jar",
    "lib_managed/jars/com.github.scala-incubator.io/scala-io-file_2.9.2/scala-io-file_2.9.2-0.4.1.jar",
    "lib_managed/jars/com.esotericsoftware.reflectasm/reflectasm/reflectasm-1.07-shaded.jar",
    "lib_managed/jars/org.eclipse.jdt/core/core-3.1.1.jar",
    "lib_managed/jars/org.spark-project/spark-core_2.9.3/spark-core_2.9.3-0.7.2.jar",
    "lib_managed/jars/org.ow2.asm/asm/asm-4.0.jar",
    "lib_managed/jars/tomcat/jasper-runtime/jasper-runtime-5.5.12.jar",
    "lib_managed/jars/tomcat/jasper-compiler/jasper-compiler-5.5.12.jar",
    "lib_managed/jars/cc.spray/spray-base/spray-base-1.0-M2.1.jar",
    "lib_managed/jars/cc.spray/spray-io/spray-io-1.0-M2.1.jar",
    "lib_managed/jars/cc.spray/spray-can/spray-can-1.0-M2.1.jar",
    "lib_managed/jars/cc.spray/spray-util/spray-util-1.0-M2.1.jar",
    "lib_managed/jars/cc.spray/spray-server/spray-server-1.0-M2.1.jar",
    "lib_managed/jars/cc.spray/spray-json_2.9.2/spray-json_2.9.2-1.1.1.jar",
    "lib_managed/jars/org.apache.mesos/mesos/mesos-0.9.0-incubating.jar",
    "lib_managed/jars/org.apache.httpcomponents/httpclient/httpclient-4.1.jar",
    "lib_managed/jars/org.apache.httpcomponents/httpcore/httpcore-4.1.jar",
    "lib_managed/jars/com.googlecode.concurrentlinkedhashmap/concurrentlinkedhashmap-lru/concurrentlinkedhashmap-lru-1.2.jar",
    "lib_managed/jars/org.eclipse.jetty/jetty-http/jetty-http-7.5.3.v20111011.jar",
    "lib_managed/jars/org.eclipse.jetty/jetty-continuation/jetty-continuation-7.5.3.v20111011.jar",
    "lib_managed/jars/org.eclipse.jetty/jetty-server/jetty-server-7.5.3.v20111011.jar",
    "lib_managed/jars/org.eclipse.jetty/jetty-util/jetty-util-7.5.3.v20111011.jar",
    "lib_managed/jars/org.eclipse.jetty/jetty-io/jetty-io-7.5.3.v20111011.jar",
    "lib_managed/jars/commons-digester/commons-digester/commons-digester-1.8.jar",
    "lib_managed/jars/commons-cli/commons-cli/commons-cli-1.2.jar",
    "lib_managed/jars/junit/junit/junit-3.8.1.jar",
    "lib_managed/jars/xmlenc/xmlenc/xmlenc-0.52.jar",
    "lib_managed/jars/commons-net/commons-net/commons-net-1.4.1.jar",
    "lib_managed/jars/org.slf4j/slf4j-api/slf4j-api-1.6.1.jar",
    "lib_managed/jars/commons-el/commons-el/commons-el-1.0.jar",
    "lib_managed/jars/com.typesafe.akka/akka-remote/akka-remote-2.0.3.jar",
    "lib_managed/jars/com.typesafe.akka/akka-slf4j/akka-slf4j-2.0.3.jar",
    "lib_managed/jars/com.typesafe.akka/akka-actor/akka-actor-2.0.3.jar",
    "lib_managed/jars/commons-beanutils/commons-beanutils-core/commons-beanutils-core-1.8.0.jar",
    "lib_managed/jars/commons-beanutils/commons-beanutils/commons-beanutils-1.7.0.jar",
    "lib_managed/jars/hsqldb/hsqldb/hsqldb-1.8.0.10.jar",
    "lib_managed/jars/com.esotericsoftware.minlog/minlog/minlog-1.2.jar",
    "lib_managed/jars/ant/ant/ant-1.6.5.jar",
    "lib_managed/jars/com.google.guava/guava/guava-11.0.1.jar",
    "lib_managed/jars/commons-io/commons-io/commons-io-1.4.jar",
    "lib_managed/jars/net.debasishg/sjson_2.9.1/sjson_2.9.1-0.15.jar",
    "lib_managed/jars/com.github.jsuereth.scala-arm/scala-arm_2.9.1/scala-arm_2.9.1-1.1.jar",
    "lib_managed/jars/commons-lang/commons-lang/commons-lang-2.6.jar",
    "lib_managed/jars/commons-logging/commons-logging/commons-logging-1.1.1.jar",
    "lib_managed/jars/org.apache.commons/commons-math/commons-math-2.1.jar",
    "lib_managed/jars/org.tomdz.twirl/twirl-api/twirl-api-1.0.2.jar",
    "lib_managed/jars/colt/colt/colt-1.2.0.jar",
    "lib_managed/jars/com.esotericsoftware.kryo/kryo/kryo-2.20-shaded.jar",
    "lib_managed/jars/net.java.dev.jets3t/jets3t/jets3t-0.7.1.jar",
    "lib_managed/jars/net.sf.kosmosfs/kfs/kfs-0.3.jar",
    "lib_managed/jars/commons-codec/commons-codec/commons-codec-1.4.jar",
    "lib_managed/bundles/log4j/log4j/log4j-1.2.17.jar",
    "lib_managed/bundles/io.netty/netty/netty-3.5.3.Final.jar",
    "lib_managed/bundles/com.ning/compress-lzf/compress-lzf-0.8.4.jar",
    "lib_managed/bundles/com.typesafe/config/config-0.3.1.jar",
    "lib_managed/bundles/de.javakaffee/kryo-serializers/kryo-serializers-0.22.jar")
}
