/**
 * 
 */
package com.logimethods.connector.nats.spark.test;

import static com.logimethods.connector.nats.spark.test.UnitTestUtilities.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.logimethods.connector.nats_spark.NatsSparkUtilities;

/**
 * @author laugimethods
 *
 */
public class NatsStreamingPubSubTest {
	protected static Logger logger = LoggerFactory.getLogger(NatsStreamingPubSubTest.class);
	
	protected final static String CLUSTER_ID = "test-cluster";
	protected final static String CLIENT_ID = "me";
	protected static String DEFAULT_SUBJECT = "NatsPubSub";
	
	@Test
	public void testPublish() throws IOException, InterruptedException, TimeoutException {
		try {
			final int nbOfMessages = 5;
			NatsStreamingPublisher np = new NatsStreamingPublisher("np", CLUSTER_ID, getUniqueClientName(), NATS_STREAMING_LOCALHOST_URL, DEFAULT_SUBJECT,  nbOfMessages);
			np.publishMessages();
		} catch (IOException e) {
			logger.error(e.getMessage() + " : " + CLUSTER_ID + " on " + NATS_STREAMING_LOCALHOST_URL);
			throw(e);
		}
	}
	
	@Test(timeout=8000)
	public void testPubSub() throws IOException, InterruptedException, TimeoutException {
		final List<Integer> data = UnitTestUtilities.getData();
		NatsStreamingSubscriber<Integer> ns = new NatsStreamingSubscriber<Integer>(NATS_STREAMING_LOCALHOST_URL, "nPub", DEFAULT_SUBJECT, CLUSTER_ID, getUniqueClientName() + "_SUB", data, Integer.class);
		NatsStreamingPublisher np = new NatsStreamingPublisher("nPub", CLUSTER_ID, getUniqueClientName() + "_PUB", NATS_STREAMING_LOCALHOST_URL, DEFAULT_SUBJECT,  data.size());
			
		ExecutorService executor = Executors.newFixedThreadPool(1);
		// start the subscribers apps
		executor.execute(ns);
		// wait for subscribers to be ready.
		ns.waitUntilReady();

		Thread.sleep(500);
		np.publishMessages();
		
		Thread.sleep(5000);
		ns.waitForCompletion();
	}

    static String getUniqueClientName() {
    	return CLIENT_ID + NatsSparkUtilities.generateUniqueID();
    }    
}
