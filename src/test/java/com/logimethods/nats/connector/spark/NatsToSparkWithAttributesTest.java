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
	protected final static String CLUSTER_ID = "CLUSTER_ID";
	protected final static String CLIENT_ID = "CLIENT_ID";
	protected final static String DURABLE_NAME = "$DURABLE_NAME";
	protected final static Properties PROPERTIES = new Properties();
	
	{
		PROPERTIES.setProperty(NATS_SUBJECTS, "sub1,sub3 , sub2");
	}

	@Test
	public void testNatsStandardToSparkConnectorImpl_1() {
		NatsStandardToSparkConnectorImpl connector = NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY())
				.withProperties(PROPERTIES).withSubjects("SUBJECT");
		assertTrue(connector instanceof NatsStandardToSparkConnectorImpl);
	}

	@Test
	public void testNatsStandardToSparkConnectorImpl_2() {
		NatsStandardToSparkConnectorImpl connector = NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY())
			.withSubjects("SUBJECT").withProperties(PROPERTIES);
		assertTrue(connector instanceof NatsStandardToSparkConnectorImpl);
	}

	@Test
	public void testNatsStreamingToSparkConnectorImpl_1() {
		NatsStreamingToSparkConnectorImpl connector = NatsToSparkConnector.receiveFromNatsStreaming(StorageLevel.MEMORY_ONLY(), CLUSTER_ID, CLIENT_ID)
				.withProperties(PROPERTIES).withSubjects("SUBJECT");
		assertTrue(connector instanceof NatsStreamingToSparkConnectorImpl);
	}

	@Test
	public void testNatsStreamingToSparkConnectorImpl_2() {
		SubscriptionOptions opts = new SubscriptionOptions.Builder().setDurableName(DURABLE_NAME).build();
		NatsStreamingToSparkConnectorImpl connector = NatsToSparkConnector.receiveFromNatsStreaming(StorageLevel.MEMORY_ONLY(), CLUSTER_ID, CLIENT_ID)
				.withProperties(PROPERTIES).withSubscriptionOptions(opts).withSubjects("SUBJECT");
		assertTrue(connector instanceof NatsStreamingToSparkConnectorImpl);
		assertEquals(DURABLE_NAME, connector.getSubscriptionOptions().getDurableName());
	}

	@Test
	public void testNatsStreamingToSparkConnectorImpl_3() {
		NatsStreamingToSparkConnectorImpl connector = NatsToSparkConnector.receiveFromNatsStreaming(StorageLevel.MEMORY_ONLY(), CLUSTER_ID, CLIENT_ID)
				.startWithLastReceived().setDurableName(DURABLE_NAME ).withSubjects("SUBJECT");
		assertTrue(connector instanceof NatsStreamingToSparkConnectorImpl);
		assertEquals(DURABLE_NAME, connector.getSubscriptionOptions().getDurableName());
	}

	@Test
	public void testNatsStreamingToSparkConnectorImpl_4() {
		SubscriptionOptions opts = new SubscriptionOptions.Builder().setDurableName(DURABLE_NAME).build();
		NatsStreamingToSparkConnectorImpl connector = NatsToSparkConnector.receiveFromNatsStreaming(StorageLevel.MEMORY_ONLY(), CLUSTER_ID, CLIENT_ID)
				.withProperties(PROPERTIES).withSubscriptionOptions(opts).setDurableName("NEW NAME").withSubjects("SUBJECT");
		assertTrue(connector instanceof NatsStreamingToSparkConnectorImpl);
		assertEquals(DURABLE_NAME, connector.getSubscriptionOptions().getDurableName());
	}

}
