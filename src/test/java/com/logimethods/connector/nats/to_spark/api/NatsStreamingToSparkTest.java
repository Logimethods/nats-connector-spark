/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.to_spark.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.LongAccumulator;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.logimethods.connector.nats.spark.test.NatsPublisher;
import com.logimethods.connector.nats.spark.test.NatsStreamingPublisher;
import com.logimethods.connector.nats.spark.test.NatsToSparkValidator;
import com.logimethods.connector.nats.spark.test.STANServer;
import com.logimethods.connector.nats.spark.test.TestClient;
import com.logimethods.connector.nats.spark.test.UnitTestUtilities;
import com.logimethods.connector.nats.to_spark.AbstractNatsToSparkTest;
import com.logimethods.connector.nats.to_spark.NatsStreamingToSparkConnectorImpl;
import com.logimethods.connector.nats.to_spark.NatsToSparkConnector;
import com.logimethods.connector.nats_spark.NatsSparkUtilities;
import com.logimethods.connector.spark.to_nats.SparkToNatsConnector;
import com.logimethods.connector.spark.to_nats.SparkToNatsStreamingConnectorPoolTest;

import io.nats.streaming.StreamingConnection;
import io.nats.streaming.Message;
import io.nats.streaming.MessageHandler;
import io.nats.streaming.NatsStreaming;
import io.nats.streaming.Options;
import io.nats.streaming.Subscription;
import io.nats.streaming.SubscriptionOptions;

import static com.logimethods.connector.nats.spark.test.UnitTestUtilities.* ;

public class NatsStreamingToSparkTest extends AbstractNatsToSparkTest {
	private static final String DURABLE_NAME = "durable-foo";
	protected final static String CLUSTER_ID = "test-cluster";
	protected final static String CLIENT_ID = "me";
	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// Enable tracing for debugging as necessary.
		Level level = Level.WARN;
		UnitTestUtilities.setLogLevel(NatsPublisher.class, level);
		UnitTestUtilities.setLogLevel(NatsStreamingPublisher.class, level);
		UnitTestUtilities.setLogLevel(SparkToNatsConnector.class, level);
		UnitTestUtilities.setLogLevel(NatsStreamingToSparkConnectorImpl.class, level);		
		UnitTestUtilities.setLogLevel(NatsStreamingToSparkTest.class, level);		
		UnitTestUtilities.setLogLevel(TestClient.class, level);
		UnitTestUtilities.setLogLevel("org.apache.spark", level);
		UnitTestUtilities.setLogLevel("org.spark-project", level);

		logger = LoggerFactory.getLogger(SparkToNatsStreamingConnectorPoolTest.class);       
	}

	@Override
	protected NatsPublisher getNatsPublisher(int nbOfMessages) {
		return new NatsStreamingPublisher("np", CLUSTER_ID, getUniqueClientName(), NATS_STREAMING_LOCALHOST_URL, DEFAULT_SUBJECT,  nbOfMessages);
	}
	
	@Test(timeout = 16000)
	public void testNatsToSparkConnectorWithAdditionalSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final JavaReceiverInputDStream<String> messages = 
				NatsToSparkConnector
						.receiveFromNatsStreaming(String.class, StorageLevel.MEMORY_ONLY(), CLUSTER_ID)
						.withNatsURL(NATS_STREAMING_URL)
						.withSubjects(DEFAULT_SUBJECT)
						.asStreamOf(ssc);

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	@Test(timeout = 16000)
	public void testNatsToKeyValueSparkConnectorWithAdditionalSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));
		
		final JavaPairDStream<String, String> messages = 
				NatsToSparkConnector
						.receiveFromNatsStreaming(String.class, StorageLevel.MEMORY_ONLY(), CLUSTER_ID)
						.withNatsURL(NATS_STREAMING_URL)
						.withSubjects(DEFAULT_SUBJECT)
						.asStreamOfKeyValue(ssc);

		validateTheReceptionOfMessages(ssc, messages);
	}

    @Test(timeout = 5000)
    public void testBasicSubscription() {
        // Run a STAN server
    	
        try (STANServer s = runServer(CLUSTER_ID, false)) {
            Options options = new Options.Builder().natsUrl(NATS_STREAMING_LOCALHOST_URL).build();
            try (StreamingConnection sc = NatsStreaming.connect(CLUSTER_ID, getUniqueClientName(), options)) {
                SubscriptionOptions sopts = new SubscriptionOptions.Builder().build();
                try (Subscription sub = sc.subscribe("foo", new MessageHandler() {
                    public void onMessage(Message msg) {}
                }, sopts)) {
                    // should have succeeded
                } catch (Exception e) {
                    fail("Expected no error on Subscribe, got: " + e.getMessage());
                }
            } catch (IOException | TimeoutException | InterruptedException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        }
    }


    @Test
    // See https://github.com/nats-io/java-nats-streaming/blob/80bf55b327e7e429959ba4cad0089ea846924da9/src/test/java/io/nats/streaming/SubscribeTests.java#L773
    public void testDurableSubscriberCloseVersusUnsub() throws Exception {
//    	if (UnitTestUtilities.TEST_MODE.equals(cluster)) {
			// TODO Generalize the usage of NatsStreamingTestServer
		//	try (NatsStreamingTestServer srv = new NatsStreamingTestServer(NATS_STREAMING_PORT, CLUSTER_ID, true)) {
				final String subject = "CloseVersusUnsub_SUBJECT_" + NatsSparkUtilities.generateUniqueID(this);
				final String queue = "CloseVersusUnsub_QUEUE_" + NatsSparkUtilities.generateUniqueID(this);

//				Options options = new Options.Builder().natsUrl(srv.getURI()).build();
				Options options = new Options.Builder().natsUrl(NATS_STREAMING_LOCALHOST_URL).build();
				final StreamingConnection natsSC = NatsStreaming.connect(CLUSTER_ID,
						CLIENT_ID + NatsSparkUtilities.generateUniqueID(this), options);

				int counter = 0;

				// Spark Client
				JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));
				final LongAccumulator accum = ssc.sparkContext().sc().longAccumulator();

				setupMessagesReception(ssc, subject, queue, DURABLE_NAME, accum);
				//    		setupMessagesReception(ssc, subject, null, DURABLE_NAME);
				//    		setupMessagesReception(ssc, subject, queue, null);

				ssc.start();
				logger.info("!!!!!!!!!! Spark Streaming Context Started");

				try {
					Thread.sleep(2000);
				} catch (Exception e) {
				}
				;

				natsSC.publish(subject, ("msg_" + String.valueOf(counter++)).getBytes(StandardCharsets.UTF_8));
				natsSC.getNatsConnection().flush(java.time.Duration.ofSeconds(1));

				try {
					Thread.sleep(2000);
				} catch (Exception e) {
				}
				; // get the ack in the queue
				ssc.close();
				sc.stop();
				sc = null;

				assertEquals(counter, accum.sum());

				natsSC.getNatsConnection().flush(java.time.Duration.ofSeconds(2));
				try {
					Thread.sleep(2000);
				} catch (Exception e) {
				}
				; // Give the server time to clean up

				/*
				 * Test reopen after close()
				 */
				natsSC.publish(subject, ("msg_" + String.valueOf(counter++)).getBytes(StandardCharsets.UTF_8));
				try {
					Thread.sleep(800);
				} catch (Exception e) {
				}
				; // get the ack in the queue
				natsSC.publish(subject, ("msg_" + String.valueOf(counter++)).getBytes(StandardCharsets.UTF_8));
				try {
					Thread.sleep(2000);
				} catch (Exception e) {
				}
				; // get the ack in the queue

				natsSC.getNatsConnection().flush(java.time.Duration.ofSeconds(2));

				// Restart a completely new Spark Context
				final SparkConf sparkConf = UnitTestUtilities.newSparkConf().setAppName("DurableSubscriberCloseVersusUnsub");
				final JavaSparkContext newSc = new JavaSparkContext(sparkConf);
				final JavaStreamingContext newSsc = new JavaStreamingContext(newSc, new Duration(200));
				final LongAccumulator newAccum = newSsc.sparkContext().sc().longAccumulator();
				
				setupMessagesReception(newSsc, subject, queue, DURABLE_NAME, newAccum); // That one should receive the waiting message
				//    		setupMessagesReception(ssc, subject, null, DURABLE_NAME); // That one should NOT receive the waiting message
				//    		setupMessagesReception(ssc, subject, queue, null); // That one should NOT receive the waiting message

				newSsc.start();
				logger.info("!!!!!!!!!! Spark Streaming Context Started AGAIN");

				try {
					Thread.sleep(4000);
				} catch (Exception e) {
				}
				; // get the ack in the queue

				newSsc.close();
				newSc.stop();
				
				assertEquals(counter, accum.sum() + newAccum.sum());
	//		} // runServer()
//		}
    }

	protected void setupMessagesReception(JavaStreamingContext ssc, String subject, String queue, String durableName, LongAccumulator accum) {
		final JavaPairDStream<String, String> messages = 
				NatsToSparkConnector
						.receiveFromNatsStreaming(String.class, StorageLevel.MEMORY_ONLY(), CLUSTER_ID)
						.withNatsURL(NATS_STREAMING_URL)
						.withSubjects(subject)
						.withNatsQueue(queue)
						.durableName(durableName)
						.asStreamOfKeyValue(ssc);
		
		NatsToSparkValidator.validateTheReceptionOfPairMessages(messages, accum);
	}

    static STANServer runServer(String clusterID) {
        return runServer(clusterID, false);
    }

    static STANServer runServer(String clusterID, boolean debug) {
        STANServer srv = new STANServer(clusterID, debug);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return srv;
    }
      
    static String getUniqueClientName() {
    	return CLIENT_ID + NatsSparkUtilities.generateUniqueID();
    }    
}
