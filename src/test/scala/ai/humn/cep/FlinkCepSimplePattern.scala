package ai.humn.cep

import java.util

import org.apache.flink.api.scala._
import org.apache.flink.cep.CEP
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.{IterativeCondition, SimpleCondition}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.scalatest.FlatSpec

import scala.collection.JavaConverters.{asJavaCollectionConverter, iterableAsScalaIterableConverter}

class FlinkCepSimplePattern extends FlatSpec {
  behavior of "Flink CEP"

  it should "match the events with Simple pattern"   in {
    //Env & Data
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val elements = Seq(
      AccelData(1L, 0, 9.81F, 0),
      AccelData(5L, 5, 9.81F, 0),
      AccelData(15L, 6, 9.81F, 0),
      AccelData(25L, 8, 9.81F, 0),
      AccelData(40L, 9, 9.81F, 0),
      AccelData(40L, 0, 9.81F, 0)
    )
    // Patterns
    val simplePattern: Pattern[AccelData, _] = Pattern.begin[AccelData]("begin").where(
      new SimpleCondition[AccelData] {
        override def filter(value: AccelData): Boolean = {
          value.x < 5.0
        }
      }
    ).followedBy("acceleration").oneOrMore().where(new SimpleCondition[AccelData] {
      override def filter(value: AccelData): Boolean = {
        value.x >= 5.0
      }
    })

    //MatchingPatterns
    val dataStream:DataStream[AccelData] = createStreamFromEvents(elements, env)
    val patternedStream = CEP.pattern(dataStream, simplePattern)
    patternedStream.process(new PatternProcessFunction[AccelData, Unit] {
      override def processMatch(`match`: util.Map[String, util.List[AccelData]], ctx: PatternProcessFunction.Context, out: Collector[Unit]): Unit = {
        print(`match`)
      }
    })
    env.execute()
  }

  it should "match the events with Simple pattern (FAILURE) " in {
    //Env & Data
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val elements = Seq(
      AccelData(1L, 0, 9.81F, 0),
      AccelData(5L, 4F, 9.81F, 0),
      AccelData(15L, 4.1F, 9.81F, 0),
      AccelData(25L, 4.8F, 9.81F, 0),
      AccelData(40L, 4.9F, 9.81F, 0),
      AccelData(40L, 4.99F, 9.81F, 0)
    )
    // Patterns
    val simplePattern: Pattern[AccelData, _] = Pattern.begin[AccelData]("begin").where(
      new SimpleCondition[AccelData] {
        override def filter(value: AccelData): Boolean = {
          value.x < 5.0
        }
      }
    ).followedBy("acceleration").oneOrMore().where(new SimpleCondition[AccelData] {
      override def filter(value: AccelData): Boolean = {
        value.x >= 5.0
      }
    })

    //MatchingPatterns
    val dataStream:DataStream[AccelData] = createStreamFromEvents(elements, env)
    val patternedStream = CEP.pattern(dataStream, simplePattern)
    patternedStream.process(new PatternProcessFunction[AccelData, Unit] {
      override def processMatch(`match`: util.Map[String, util.List[AccelData]], ctx: PatternProcessFunction.Context, out: Collector[Unit]): Unit = {
        print(`match`)
      }
    })
    env.execute()
  }

  it should "match the events with Iterative Pattern with initial filter" in {
    //Env & Data
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val elements = Seq(
      AccelData(1L, 0, 9.81F, 0),
      AccelData(5L, 5, 9.81F, 0),
      AccelData(15L, 6, 9.81F, 0),
      AccelData(25L, 8, 9.81F, 0),
      AccelData(40L, 9, 9.81F, 0),
      AccelData(40L, 10, 9.81F, 0)
    )
    // Patterns
    val simplePattern: Pattern[AccelData, _] = Pattern.begin[AccelData]("begin").where(
      new SimpleCondition[AccelData] {
        override def filter(value: AccelData): Boolean = {
          value.x < 4.0
        }
      }
    ).followedBy("acceleration").oneOrMore().where(new IterativeCondition[AccelData] {
      override def filter(value: AccelData, ctx: IterativeCondition.Context[AccelData]): Boolean = {
        ctx.getEventsForPattern("acceleration").asScala.forall(_.x < value.x)
      }
    })

    //MatchingPatterns
    val dataStream:DataStream[AccelData] = createStreamFromEvents(elements, env)
    val patternedStream = CEP.pattern(dataStream, simplePattern)
    patternedStream.process(new PatternProcessFunction[AccelData, Unit] {
      override def processMatch(`match`: util.Map[String, util.List[AccelData]], ctx: PatternProcessFunction.Context, out: Collector[Unit]): Unit = {
        print(`match`)
      }
    })
    env.execute()
  }


  it should "match the events with Iterative Pattern without initial filter" in {
    //Env & Data
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val elements = Seq(
      AccelData(1L, 0, 9.81F, 0),
      AccelData(5L, 5, 9.81F, 0),
      AccelData(15L, 6, 9.81F, 0),
      AccelData(25L, 8, 9.81F, 0),
      AccelData(40L, 9, 9.81F, 0),
      AccelData(40L, 10, 9.81F, 0)
    )
    // Patterns
    val simplePattern: Pattern[AccelData, _] = Pattern.begin[AccelData]("begin").followedBy("acceleration").oneOrMore().where(new IterativeCondition[AccelData] {
      override def filter(value: AccelData, ctx: IterativeCondition.Context[AccelData]): Boolean = {
        ctx.getEventsForPattern("acceleration").asScala.forall(_.x < value.x)
      }
    })

    //MatchingPatterns
    val dataStream:DataStream[AccelData] = createStreamFromEvents(elements, env)
    val patternedStream = CEP.pattern(dataStream, simplePattern)
    patternedStream.process(new PatternProcessFunction[AccelData, Unit] {
      override def processMatch(`match`: util.Map[String, util.List[AccelData]], ctx: PatternProcessFunction.Context, out: Collector[Unit]): Unit = {
        print(`match`)
      }
    })
    env.execute()
  }

  it should "match the events with Iterative Pattern with time constraint" in {
    //Env & Data
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val elements = Seq(
      AccelData(1L, 0, 9.81F, 0),
      AccelData(5L, 5, 9.81F, 0),
      AccelData(15L, 6, 9.81F, 0),
      AccelData(25L, 8, 9.81F, 0),
      AccelData(40L, 9, 9.81F, 0),
      AccelData(60L, 10, 9.81F, 0)
    )
    // Patterns
    val simplePattern: Pattern[AccelData, _] = Pattern.begin[AccelData]("begin").where(
      new SimpleCondition[AccelData] {
        override def filter(value: AccelData): Boolean = {
          value.x < 4.0
        }
      }
    ).followedBy("acceleration").oneOrMore().where(new IterativeCondition[AccelData] {
      override def filter(value: AccelData, ctx: IterativeCondition.Context[AccelData]): Boolean = {
        ctx.getEventsForPattern("acceleration").asScala.forall(_.x < value.x)
      }
    }).within(Time.milliseconds(30L))

    //MatchingPatterns
    val dataStream:DataStream[AccelData] = createStreamFromEvents(elements, env)
    val patternedStream = CEP.pattern(dataStream, simplePattern)
    patternedStream.process(new PatternProcessFunction[AccelData, Unit] {
      override def processMatch(`match`: util.Map[String, util.List[AccelData]], ctx: PatternProcessFunction.Context, out: Collector[Unit]): Unit = {
        print(`match`)
      }
    })
    env.execute()
  }

  private def createStreamFromEvents(accelSeq: Seq[AccelData], env: StreamExecutionEnvironment) = {
    env.fromCollection(accelSeq.asJavaCollection)
      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[AccelData] {
        override def checkAndGetNextWatermark(lastElement: AccelData, extractedTimestamp: Long): Watermark = new Watermark(lastElement.timestamp)

        override def extractTimestamp(element: AccelData, previousElementTimestamp: Long): Long = element.timestamp
      })
  }
}
