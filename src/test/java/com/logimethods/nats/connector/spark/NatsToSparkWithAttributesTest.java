/**
 * 
 */
package com.logimethods.nats.connector.spark;

import static com.logimethods.nats.connector.spark.NatsToSparkConnector.NATS_SUBJECTS;
import static org.junit.Assert.*;

import java.util.Properties;

import org.apache.spark.storage.StorageLevel;
import org.junit.Test;

import io.nats.stan.SubscriptionOptions;

/**
 * @author laugimethods
 *
 */
public class NatsToSparkWithAttributesTest {

	@Test
	public void testNatsStandardToSparkConnectorImpl() {
		final Properties properties = new Properties();
		properties.setProperty(NATS_SUBJECTS, "sub1,sub3 , sub2");
		NatsToSparkConnector connector = NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY())
				.withProperties(properties).withSubjects("SUBJECT");
		assertTrue(connector instanceof NatsStandardToSparkConnectorImpl);
		
		connector = NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY())
				.withSubjects("SUBJECT").withProperties(properties);
		assertTrue(connector instanceof NatsStandardToSparkConnectorImpl);
	}

	@Test
	public void testNatsStreamingToSparkConnectorImpl() {
		final Properties properties = new Properties();
		properties.setProperty(NATS_SUBJECTS, "sub1,sub3 , sub2");
		String clusterID = "clusterID";
		String clientID = "clientID";
		NatsToSparkConnector connector = NatsToSparkConnector.receiveFromNatsStreaming(StorageLevel.MEMORY_ONLY(), clusterID, clientID)
				.withProperties(properties).withSubjects("SUBJECT");
		assertTrue(connector instanceof NatsStreamingToSparkConnectorImpl);
		
		SubscriptionOptions opts = new SubscriptionOptions.Builder().setDurableName("my-durable").build();
		connector = NatsToSparkConnector.receiveFromNatsStreaming(StorageLevel.MEMORY_ONLY(), clusterID, clientID)
				.withProperties(properties).withSubscriptionOptions(opts).withSubjects("SUBJECT");
		assertTrue(connector instanceof NatsStreamingToSparkConnectorImpl);
	}

}
