package com.logimethods.nats.connector.spark.subscribe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.logimethods.nats.connector.spark.NatsPublisher;
import com.logimethods.nats.connector.spark.TestClient;
import com.logimethods.nats.connector.spark.UnitTestUtilities;

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
		
		DEFAULT_SUBJECT = DEFAULT_SUBJECT_ROOT + (DEFAULT_SUBJECT_INR++);
		TOTAL_COUNT.set(0);
		
		rightNumber = true;
		atLeastSomeData = false;
		
		SparkConf sparkConf = new SparkConf().setAppName("My Spark Job").setMaster("local[2]");
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
		
		messages.foreachRDD(new Function<JavaRDD<String>, Void>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Void call(JavaRDD<String> rdd) throws Exception {
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
				
				return null;
			}			
		});
		
		ssc.start();		
		Thread.sleep(1000);		
		// start the publisher
		executor.execute(np);
		np.waitUntilReady();		
		Thread.sleep(500);
		ssc.close();		
		Thread.sleep(500);
		assertTrue("Not a single RDD did received messages.", atLeastSomeData);	
		assertTrue("Not the right number of messages have been received", rightNumber);
		assertEquals(nbOfMessages, TOTAL_COUNT.get());
		assertNull("'" + payload + " should be '" + NatsPublisher.NATS_PAYLOAD + "'", payload);		
	}

	protected abstract NatsPublisher getNatsPublisher(final int nbOfMessages);
}
