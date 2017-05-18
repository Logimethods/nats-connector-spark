package com.logimethods.connector.spark.to_nats;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Test;

import io.nats.streaming.Options;
import io.nats.streaming.Options.Builder;
import io.nats.streaming.OptionsHelper;

public class SparkToNatsStreamingConnectorTest {

	private static final String natsURL = "nats://123.123.123.123:4444";
	private static final Properties properties = new Properties();
	private static final Collection<String> subjects = Arrays.asList("Hello", "World!");
	private static final boolean isStoredAsKeyValue = true;
	private static final String clusterID = "ClusterID";
	private static final Long connectionTimeout = 111l;

	@Test
	// @See https://github.com/Logimethods/nats-connector-spark/pull/3
	// @See https://github.com/nats-io/java-nats-streaming/issues/51
	public void testSparkToStandardNatsConnectorImpl_Serialization() throws Exception {
		final Builder optionsBuilder = new Options.Builder().natsUrl(natsURL);
		final SparkToNatsStreamingConnectorImpl source = 
				new SparkToNatsStreamingConnectorImpl(clusterID, natsURL, properties, connectionTimeout, optionsBuilder, subjects, isStoredAsKeyValue);
		
		final SparkToNatsStreamingConnectorImpl target = SerializationUtils.clone(source);
		
		assertEquals(clusterID, target.clusterID);
		assertEquals(source.getNatsURL(), target.getNatsURL());
		assertEquals(source.getProperties(), target.getProperties());
		assertEquals(source.getSubjects(), target.getSubjects());
		assertEquals(connectionTimeout, target.connectionTimeout);
		assertEquals(isStoredAsKeyValue, target.isStoredAsKeyValue());
		assertEquals(OptionsHelper.extractNatsUrl(source.getOptionsBuilder().build()), 
						OptionsHelper.extractNatsUrl(target.getOptionsBuilder().build()));
	}
	
}
