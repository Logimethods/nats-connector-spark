/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.nats.connector.spark.subscribe;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeoutException;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Test;

import com.logimethods.nats.connector.spark.NatsPublisher;
import com.logimethods.nats.connector.spark.STANServer;

import io.nats.stan.Connection;
import io.nats.stan.ConnectionFactory;
import io.nats.stan.Message;
import io.nats.stan.MessageHandler;
import io.nats.stan.Subscription;
import io.nats.stan.SubscriptionOptions;

public class StreamingNatsToSparkTest extends AbstractNatsToSparkTest {
	protected final static String CLUSTER_ID = "test-cluster";
	protected final static String CLIENT_ID = "CLIENT_ID";
	private static final int STANServerPORT = 4223;
	private static final String STAN_URL = "nats://localhost:" + STANServerPORT;
	
//	@Test
	public void testNatsToSparkConnectorWithAdditionalSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final JavaReceiverInputDStream<String> messages = 
				ssc.receiverStream(NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY()).withSubjects(DEFAULT_SUBJECT));

		validateTheReceptionOfMessages(ssc, messages);
	}

    @Test
    public void testBasicSubscription() {
        // Run a STAN server
        try (STANServer s = runServer(CLUSTER_ID, false)) {
            ConnectionFactory cf = new ConnectionFactory(CLUSTER_ID, CLIENT_ID + (new Date().getTime()));
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

	@Override
	protected NatsPublisher getNatsPublisher(int nbOfMessages) {
		// TODO Auto-generated method stub
		return null;
	}
}
