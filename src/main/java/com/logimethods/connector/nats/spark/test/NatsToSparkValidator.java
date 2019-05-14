package com.logimethods.connector.nats.spark.test;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class NatsToSparkValidator implements Serializable {
	private static final long serialVersionUID = 1L;
	
	protected static String DEFAULT_SUBJECT_ROOT = "nats2sparkSubject";
	protected static int DEFAULT_SUBJECT_INR = 0;
	protected static String DEFAULT_SUBJECT;
	protected static JavaSparkContext sc;
	public static AtomicInteger TOTAL_COUNT = new AtomicInteger();
	protected static final Logger logger = LoggerFactory.getLogger(NatsToSparkValidator.class);
	protected static Boolean rightNumber = true;
	protected static Boolean atLeastSomeData = false;
	protected static String payload = null;
	
/*	protected void validateTheReceptionOfMessages(JavaStreamingContext ssc,
			JavaReceiverInputDStream<String> stream) throws InterruptedException {
		JavaDStream<String> messages = stream.repartition(3);

		ExecutorService executor = Executors.newFixedThreadPool(6);

		final int nbOfMessages = 5;
		NatsPublisher np = getNatsPublisher(nbOfMessages);
		
		if (logger.isDebugEnabled()) {
			messages.print();
		}
		
		messages.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<String> rdd) throws Exception {
				logger.debug("RDD received: {}", rdd.collect());
				
				final long count = rdd.count();
				if ((count != 0) && (count != nbOfMessages)) {
					rightNumber = false;
					logger.error("The number of messages received should have been {} instead of {}.", nbOfMessages, count);
				}
				
				TOTAL_COUNT.getAndAdd((int) count);
				
				atLeastSomeData = atLeastSomeData || (count > 0);
				
				for (String str :rdd.collect()) {
					if (! str.startsWith(NatsPublisher.NATS_PAYLOAD)) {
							payload = str;
						}
				}
			}			
		});
		
		closeTheValidation(ssc, executor, nbOfMessages, np);		
	}
	

	protected void validateTheReceptionOfIntegerMessages(JavaStreamingContext ssc, 
			JavaReceiverInputDStream<Integer> stream) throws InterruptedException {
		JavaDStream<Integer> messages = stream.repartition(3);

		ExecutorService executor = Executors.newFixedThreadPool(6);

		final int nbOfMessages = 5;
		NatsPublisher np = getNatsPublisher(nbOfMessages);
		
//		if (logger.isDebugEnabled()) {
			messages.print();
//		}
		
		messages.foreachRDD(new VoidFunction<JavaRDD<Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<Integer> rdd) throws Exception {
				logger.debug("RDD received: {}", rdd.collect());
System.out.println("RDD received: " + rdd.collect());				
				final long count = rdd.count();
				if ((count != 0) && (count != nbOfMessages)) {
					rightNumber = false;
					logger.error("The number of messages received should have been {} instead of {}.", nbOfMessages, count);
				}
				
				TOTAL_COUNT.getAndAdd((int) count);
				
				atLeastSomeData = atLeastSomeData || (count > 0);
				
				for (Integer value :rdd.collect()) {
					if (value < NatsPublisher.NATS_PAYLOAD_INT) {
							payload = value.toString();
						}
				}
			}			
		});
		
		closeTheValidation(ssc, executor, nbOfMessages, np);
	}
*/
	public static void validateTheReceptionOfIntegerMessages(final JavaDStream<Integer> messages, final LongAccumulator count) {
		messages.count().foreachRDD(rdd -> rdd.foreach(n -> count.add(n)));
	}

	public static void validateTheReceptionOfMessages(final JavaPairDStream<String, String> messages, final LongAccumulator count) {
		messages.count().foreachRDD(rdd -> rdd.foreach(n -> count.add(n)));
	}

	public static void validateTheReceptionOfPairMessages(final JavaPairDStream<String, String> messages, final LongAccumulator accum) {
		// messages.print();

		JavaPairDStream<String, Integer> pairs = messages.mapToPair(s -> new Tuple2(s._1, 1));		
		JavaPairDStream<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);

		// counts.print();

		counts.foreachRDD((VoidFunction<JavaPairRDD<String, Integer>>) pairRDD -> {
			pairRDD.foreach((VoidFunction<Tuple2<String, Integer>>) tuple -> {
				logger.info("{} RECEIVED", tuple);
				final long count = tuple._2;        				
				accum.add(count);
			});
		});
	}


}
