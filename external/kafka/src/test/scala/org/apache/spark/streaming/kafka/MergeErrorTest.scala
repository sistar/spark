package org.apache.spark.streaming.kafka


import _root_.kafka.serializer.StringDecoder
import org.apache.log4j.{Logger, Level}
import org.apache.spark._
import org.apache.spark.rdd
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming._
import org.apache.spark.util.ManualClock
import org.scalatest.concurrent.Eventually
import org.scalatest.{WordSpecLike, Matchers}

object SparkUtil {

  def silenceSpark() = {
    setLogLevels(Level.WARN, Seq("spark", "org.eclipse.jetty", "akka"))
  }

  def setLogLevels(level: Level, loggers: TraversableOnce[String]) = {
    loggers.map{
      loggerName =>
        val logger = Logger.getLogger(loggerName)
        val prevLevel = logger.getLevel
        logger.setLevel(level)
        loggerName -> prevLevel
    }.toMap
  }

}

object StreamingTestClock extends StreamingTestClock

class TestClock(private val clock:ManualClock) {
  def getTimeMillis():Long = clock.getTimeMillis()
  def setTime(timeToSet : scala.Long) : Unit = clock.setTime(timeToSet)
  def advance(timeToAdd : scala.Long) : Unit = clock.advance(timeToAdd)
  def waitTillTime(targetTime : scala.Long) : scala.Long = clock.waitTillTime(targetTime)
}

trait StreamingTestClock {

  implicit class StreamingContextWithClock(ssc:StreamingContext)  {
    def clock = {
      ssc.scheduler.clock
      extractClock(ssc)
    }
  }

  def extractClock(ssc: StreamingContext): TestClock = {
    new TestClock(ssc.scheduler.clock.asInstanceOf[ManualClock])
  }


}

trait SparkTest {

  def baseConf = new SparkConf()
    .setMaster("local[4]")
    .set("spark.cores", "4")
    .set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec")
    .setAppName(this.getClass.getSimpleName)

  /**
   * convenience method for tests that use spark. Creates a local spark context,
   * and cleans it up even if your test fails.
   *
   * By default, it turn off spark logging, b/c it just clutters up the test output.
   * However, when you are actively debugging one test, you may want to turn the logs on
   *
   * @param silenceSpark true to turn off spark logging
   *
   */
  def sparkTest(silenceSpark: Boolean, conf: SparkConf = baseConf)(body: (SparkContext) => Unit) = {
    val origLogLevels = if (silenceSpark) SparkUtil.silenceSpark() else null
    val _sc = new SparkContext(conf)
    try {
      body(_sc)
    } finally {
      _sc.stop()
      // To avoid Akka rebinding to the same port,
      // since it doesn't unbind immediately on shutdown
      System.clearProperty("spark.master.port")
      if (silenceSpark) {
        origLogLevels.foreach { case (name, level) => SparkUtil.setLogLevels(level, Seq(name)) }

      }
    }
  }
}


trait StreamingTest extends SparkTest {

  def streamingTest(batchDuration:Duration = Duration(500), conf:SparkConf = baseConf)(body: (SparkContext, StreamingContext, TestClock)=>Unit) = {
    sparkTest(silenceSpark = true, conf.set("spark.streaming.clock", "org.apache.spark.util.ManualClock")){ sc =>
      val ssc = new StreamingContext(sc, batchDuration)
      ssc.checkpoint("/tmp")
      try {
        body(sc, ssc, StreamingTestClock.extractClock(ssc))
      }
      finally {
        ssc.stop(stopSparkContext = false, stopGracefully = false)
      }
    }
  }
}

class MergeErrorTest extends StreamingTest with Matchers with WordSpecLike with Eventually {

  "Merger" should {

    val batchMs = 200
    val windowTimesBatch = 2

    "merge tracking and order message with same ssid in same partition" in streamingTest(batchDuration = Milliseconds(batchMs)) { (sc, ssc, clock) =>

      val properties = Map(
        "metadata.broker.list" -> "localhost:9092"
      )


      val stream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder ](ssc, properties,Set("tracking"))
      val windowedStream: DStream[(String, String)] = stream.map(x=>x).window(Milliseconds(batchMs*windowTimesBatch))

      windowedStream.foreachRDD { rdd =>
        println(rdd)
        rdd.foreach {
          case (x: String, y: String) => println(s"$x")
          case _ =>
        }
        rdd.collect()
      }
      ssc.start()

      while (true) {
        Thread.sleep(1000)
        clock.advance(1000)
      }

    }

  }

}