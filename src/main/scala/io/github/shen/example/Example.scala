package io.github.shen.example

import io.github.shen.output.KafkaOutputBeam
import io.github.shen.streaming.StreamingJob
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import com.github.benfradet.spark.kafka010.writer._

/**
  * Created by shen on 8/4/17.
  */
object WordCount {
  type WordCount = (String, Int)
  def main(args: Array[String]): Unit = {
    // Initialize beam
    val path = getClass.getResource("/exampleBeam.conf").getPath
    val streamingJob = new StreamingJob(path)

    // read from kafka topcis
    val lines = streamingJob.inputBeams(0).read().map(rec => rec.value())

    // apply transformation on dstream
    val words = lines
      .transform(splitLine)
      .transform(skipEmptyWords)
      .transform(toLowerCase)
      //.transform(skipStopWords(stopWordsVar))

    val windowDurationVar = streamingJob.jobConfig.windowDuration
    val slideDurationVar = streamingJob.jobConfig.slideDuration
    val wordCounts = words
      .map(word => (word, 1))
      .reduceByKeyAndWindow(_ + _, _ - _, windowDurationVar, slideDurationVar)

    wordCounts.transform(sortWordCounts)
    //.transform(skipEmptyWordCounts)

    // write to kafka topics
    val kafkaOutputBeam = streamingJob.outputBeams(0).asInstanceOf[KafkaOutputBeam]
    wordCounts.writeToKafka(
      kafkaOutputBeam.producerConfig,
      s => new ProducerRecord[String, String](kafkaOutputBeam.kafkaTopics(0), s.toString())
    )

    streamingJob.startAndAwaitTermination()
  }

  val toLowerCase = (words: RDD[String]) => words.map(word => word.toLowerCase)

  val splitLine = (lines: RDD[String]) => lines.flatMap(line => line.split("[^\\p{L}]"))

  val skipEmptyWords = (words: RDD[String]) => words.filter(word => !word.isEmpty)

  val skipStopWords = (stopWords: Broadcast[Set[String]]) => (words: RDD[String]) =>
    words.filter(word => !stopWords.value.contains(word))

  val skipEmptyWordCounts = (wordCounts: RDD[WordCount]) => wordCounts.filter(wordCount => wordCount._2 > 0)

  val sortWordCounts = (wordCounts: RDD[WordCount]) => wordCounts.sortByKey()
}
