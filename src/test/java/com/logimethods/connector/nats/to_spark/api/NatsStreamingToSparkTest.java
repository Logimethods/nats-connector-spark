/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.to_spark.api;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Level;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.logimethods.connector.nats.spark.test.NatsPublisher;
import com.logimethods.connector.nats.spark.test.NatsStreamingPublisher;
import com.logimethods.connector.nats.spark.test.STANServer;
import com.logimethods.connector.nats.spark.test.TestClient;
import com.logimethods.connector.nats.spark.test.UnitTestUtilities;
import com.logimethods.connector.nats.to_spark.NatsStreamingToKeyValueSparkConnectorImpl;
import com.logimethods.connector.nats.to_spark.NatsStreamingToSparkConnectorImpl;
import com.logimethods.connector.nats.to_spark.NatsToSparkConnector;
import com.logimethods.connector.nats_spark.NatsSparkUtilities;
import com.logimethods.connector.spark.to_nats.SparkToNatsConnector;
import com.logimethods.connector.spark.to_nats.api.SparkToNatsStreamingConnectorPoolTest;

import io.nats.stan.Connection;
import io.nats.stan.ConnectionFactory;
import io.nats.stan.Message;
import io.nats.stan.MessageHandler;
import io.nats.stan.Subscription;
import io.nats.stan.SubscriptionOptions;
import scala.Tuple2;

public class NatsStreamingToSparkTest extends AbstractNatsToSparkTest {
	protected final static String CLUSTER_ID = "test-cluster";
	protected final static String CLIENT_ID = "CLIENT_ID";
	private static final int STANServerPORT = 4223;
	private static final String STAN_URL = "nats://localhost:" + STANServerPORT;
	
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
		return new NatsStreamingPublisher("np", CLUSTER_ID, getUniqueClientName(), STAN_URL, DEFAULT_SUBJECT,  nbOfMessages);
	}
	
	@Test(timeout = 8000)
	public void testNatsToSparkConnectorWithAdditionalSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final JavaReceiverInputDStream<String> messages = 
				NatsToSparkConnector
						.receiveFromNatsStreaming(String.class, StorageLevel.MEMORY_ONLY(), CLUSTER_ID)
						.withNatsURL(STAN_URL)
						.withSubjects(DEFAULT_SUBJECT)
						.asStreamOf(ssc);

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	@Test(timeout = 8000)
	public void testNatsToKeyValueSparkConnectorWithAdditionalSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));
		
		final JavaPairDStream<String, String> messages = 
				NatsToSparkConnector
						.receiveFromNatsStreaming(String.class, StorageLevel.MEMORY_ONLY(), CLUSTER_ID)
						.withNatsURL(STAN_URL)
						.withSubjects(DEFAULT_SUBJECT)
						.storedAsKeyValue()
						.asStreamOf(ssc);

		validateTheReceptionOfMessages(ssc, messages);
	}

    @Test(timeout = 5000)
    public void testBasicSubscription() {
        // Run a STAN server
    	
        try (STANServer s = runServer(CLUSTER_ID, false)) {
            ConnectionFactory cf = new ConnectionFactory(CLUSTER_ID, getUniqueClientName());
            cf.setNatsUrl(STAN_URL);
            try (Connection sc = cf.createConnection()) {
                SubscriptionOptions sopts = new SubscriptionOptions.Builder().build();
                try (Subscription sub = sc.subscribe("foo", new MessageHandler() {
                    public void onMessage(Message msg) {}
                }, sopts)) {
                    // should have succeeded
                } catch (Exception e) {
                    fail("Expected no error on Subscribe, got: " + e.getMessage());
                }
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        }
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
