package com.logimethods.connector.nats.to_spark;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.LongAccumulator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.logimethods.connector.nats.spark.test.NatsPublisher;
import com.logimethods.connector.nats.spark.test.NatsToSparkValidator;
import com.logimethods.connector.nats.spark.test.TestClient;
import com.logimethods.connector.nats.spark.test.UnitTestUtilities;
import com.logimethods.connector.nats.to_spark.api.StandardNatsToSparkConnectorTest;

public abstract class AbstractNatsToSparkTest {
	
	protected static String DEFAULT_SUBJECT_ROOT = "nats2sparkSubject";
	protected static int DEFAULT_SUBJECT_INR = 0;
	protected static String DEFAULT_SUBJECT;
	protected static JavaSparkContext sc;
//	protected static AtomicInteger TOTAL_COUNT = new AtomicInteger();
	protected static Logger logger = null;
	protected static Boolean rightNumber = true;
	protected static Boolean atLeastSomeData = false;
	protected static String payload = null;
	
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
		NatsToSparkValidator.TOTAL_COUNT.set(0);
		
		rightNumber = true;
		atLeastSomeData = false;
		
		 // https://stackoverflow.com/questions/41864985/hadoop-ioexception-failure-to-login
//		UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("sparkuser"));
		
		SparkConf sparkConf = 
				UnitTestUtilities.newSparkConf()
					.setAppName("My Spark Job");
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
				
				NatsToSparkValidator.TOTAL_COUNT.getAndAdd((int) count);
				
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
		
/*		messages.foreachRDD(new VoidFunction<JavaRDD<Integer>>() {
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
				
				NatsToSparkValidator.TOTAL_COUNT.getAndAdd((int) count);
				
				atLeastSomeData = atLeastSomeData || (count > 0);
				
				for (Integer value :rdd.collect()) {
					if (value < NatsPublisher.NATS_PAYLOAD_INT) {
							payload = value.toString();
						}
				}
			}			
		});*/
		
		final LongAccumulator count = ssc.sparkContext().sc().longAccumulator();
		NatsToSparkValidator.validateTheReceptionOfIntegerMessages(messages, count);
		
		closeTheValidation(ssc, executor, nbOfMessages, np);
		assertEquals(nbOfMessages, count.sum());
	}

	protected void validateTheReceptionOfMessages(final JavaStreamingContext ssc,
			final JavaPairDStream<String, String> messages) throws InterruptedException {

		ExecutorService executor = Executors.newFixedThreadPool(6);

		final int nbOfMessages = 5;
		NatsPublisher np = getNatsPublisher(nbOfMessages);
		
		if (logger.isDebugEnabled()) {
			messages.print();
		}

		final LongAccumulator count = ssc.sparkContext().sc().longAccumulator();
		NatsToSparkValidator.validateTheReceptionOfMessages(messages, count);
		
		closeTheValidation(ssc, executor, nbOfMessages, np);		
		assertEquals(nbOfMessages, count.sum());
	}

	protected void closeTheValidation(JavaStreamingContext ssc, ExecutorService executor, final int nbOfMessages,
			NatsPublisher np) throws InterruptedException {
		ssc.start();		
		Thread.sleep(1000);
		// start the publisher
		executor.execute(np);
		np.waitUntilReady();		
		Thread.sleep(2000);
		ssc.close();		
		Thread.sleep(2000);
	}

	protected abstract NatsPublisher getNatsPublisher(final int nbOfMessages);
}
