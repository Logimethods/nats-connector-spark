package com.logimethods.connector.nats.to_spark;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.spark.storage.StorageLevel;
import org.junit.Test;

import io.nats.streaming.SubscriptionOptions;

@SuppressWarnings("serial")
public class NatsStreamingToSparkConnectorTest implements Serializable {
    private static final String DURABLE_NAME = "DURABLE_NAME";
    
	@Test
	// @See https://github.com/Logimethods/nats-connector-spark/pull/3
	// @See https://github.com/nats-io/java-nats-streaming/issues/51
	public void testSubscriptionOptions_BuilderSerialization() throws IOException, ClassNotFoundException {
		SubscriptionOptions.Builder optsBuilder = new SubscriptionOptions.Builder().durableName(DURABLE_NAME);

		final SubscriptionOptions.Builder newOptsBuilder = (SubscriptionOptions.Builder) SerializationUtils.clone(optsBuilder);
		
		assertEquals(DURABLE_NAME, newOptsBuilder.build().getDurableName());
	}

	@Test
	// @See https://github.com/Logimethods/nats-connector-spark/pull/3
	// @See https://github.com/nats-io/java-nats-streaming/issues/51
	public void testNatsStreamingToSparkConnectorImpl_Serialization() throws IOException, ClassNotFoundException {
		SubscriptionOptions.Builder optsBuilder = new SubscriptionOptions.Builder().durableName(DURABLE_NAME);
		final NatsStreamingToSparkConnectorImpl<String> connector = 
				NatsToSparkConnector
				.receiveFromNatsStreaming(String.class, StorageLevel.MEMORY_ONLY(), "clusterID") 
				.withSubscriptionOptionsBuilder(optsBuilder)
				.deliverAllAvailable() 
				.withNatsURL("NATS_URL") 
				.withSubjects("DEFAULT_SUBJECT");
	
		@SuppressWarnings("unchecked")
		final NatsStreamingToSparkConnectorImpl<String> newConnector = (NatsStreamingToSparkConnectorImpl<String>) SerializationUtils.clone(connector);
		
		assertEquals(DURABLE_NAME, newConnector.getSubscriptionOptions().getDurableName());
	}
}

