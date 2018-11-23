package com.logimethods.connector.nats.to_spark.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.logimethods.connector.nats.spark.test.NatsPublisher;
import com.logimethods.connector.nats.spark.test.TestClient;
import com.logimethods.connector.nats.spark.test.UnitTestUtilities;
import com.logimethods.connector.nats.to_spark.NatsToSparkConnector;

import scala.Tuple2;

public abstract class AbstractNatsToSparkTest {
	protected static String DEFAULT_SUBJECT_ROOT = "nats2sparkSubject";
	protected static int DEFAULT_SUBJECT_INR = 0;
	protected static String DEFAULT_SUBJECT;
	protected static JavaSparkContext sc;
	protected static AtomicInteger TOTAL_COUNT = new AtomicInteger();
	static Logger logger = null;
	static Boolean rightNumber = true;
	static Boolean atLeastSomeData = false;
	static String payload = null;
	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Enable tracing for debugging as necessary.
		Level level = Level.WARN;
		UnitTestUtilities.setLogLevel(NatsToSparkConnector.class, level);
		UnitTestUtilities.setLogLevel(StandardNatsToSparkConnectorTest.class, level);
		UnitTestUtilities.setLogLevel(TestClient.class, level);
		UnitTestUtilities.setLogLevel("org.apache.spark", level);
		UnitTestUtilities.setLogLevel("org.spark-project", level);

		logger = LoggerFactory.getLogger(StandardNatsToSparkConnectorTest.class);       

		UnitTestUtilities.startDefaultServer();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		UnitTestUtilities.stopDefaultServer();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
//		assertTrue(logger.isDebugEnabled());
//		assertTrue(LoggerFactory.getLogger(NatsToSparkConnector.class).isTraceEnabled());
		
		// To avoid "Only one StreamingContext may be started in this JVM. Currently running StreamingContext was started at .../..."
		Thread.sleep(500);
		
		DEFAULT_SUBJECT = DEFAULT_SUBJECT_ROOT + (DEFAULT_SUBJECT_INR++);
		TOTAL_COUNT.set(0);
		
		rightNumber = true;
		atLeastSomeData = false;
		
		SparkConf sparkConf = new SparkConf().setAppName("My Spark Job").setMaster("local[2]").set("spark.driver.host", "localhost"); // https://issues.apache.org/jira/browse/
		sc = new JavaSparkContext(sparkConf);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		sc.stop();
	}
	
	protected void validateTheReceptionOfMessages(JavaStreamingContext ssc,
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
		
		if (logger.isDebugEnabled()) {
			messages.print();
		}
		
		messages.foreachRDD(new VoidFunction<JavaRDD<Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<Integer> rdd) throws Exception {
				logger.debug("RDD received: {}", rdd.collect());
				
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

	protected void validateTheReceptionOfMessages(final JavaStreamingContext ssc,
			final JavaPairDStream<String, String> messages) throws InterruptedException {

		ExecutorService executor = Executors.newFixedThreadPool(6);

		final int nbOfMessages = 5;
		NatsPublisher np = getNatsPublisher(nbOfMessages);
		
		if (logger.isDebugEnabled()) {
			messages.print();
		}
		
		JavaPairDStream<String, Integer> pairs = messages.mapToPair(s -> new Tuple2(s._1, 1));		
		JavaPairDStream<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
		counts.print();
		
		counts.foreachRDD((VoidFunction<JavaPairRDD<String, Integer>>) pairRDD -> {
			pairRDD.foreach((VoidFunction<Tuple2<String, Integer>>) tuple -> {
				final long count = tuple._2;
				if ((count != 0) && (count != nbOfMessages)) {
					rightNumber = false;
					logger.error("The number of messages received should have been {} instead of {}.", nbOfMessages, count);
				}

				TOTAL_COUNT.getAndAdd((int) count);

				atLeastSomeData = atLeastSomeData || (count > 0);
			});
		});
		
		closeTheValidation(ssc, executor, nbOfMessages, np);		
	}

	protected void closeTheValidation(JavaStreamingContext ssc, ExecutorService executor, final int nbOfMessages,
			NatsPublisher np) throws InterruptedException {
		ssc.start();		
		Thread.sleep(1000);		
		// start the publisher
		executor.execute(np);
		np.waitUntilReady();		
		Thread.sleep(1000);
		ssc.close();		
		Thread.sleep(1000);
		assertTrue("Not a single RDD did received messages.", atLeastSomeData);	
		assertTrue("Not the right number of messages have been received", rightNumber);
		assertEquals(nbOfMessages, TOTAL_COUNT.get());
		assertNull("'" + payload + " should be '" + NatsPublisher.NATS_PAYLOAD + "'", payload);
	}

	protected abstract NatsPublisher getNatsPublisher(final int nbOfMessages);
}
