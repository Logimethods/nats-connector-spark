/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.to_spark.api;

import static com.logimethods.connector.nats.spark.test.UnitTestUtilities.NATS_SERVER_URL;
import static com.logimethods.connector.nats_spark.Constants.PROP_SUBJECTS;
import static io.nats.client.Nats.PROP_URL;

import java.io.Serializable;
import java.util.Properties;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Test;

import com.logimethods.connector.nats.spark.test.NatsPublisher;
import com.logimethods.connector.nats.spark.test.StandardNatsPublisher;
import com.logimethods.connector.nats.to_spark.NatsToSparkConnector;

public class StandardNatsToSparkKeyValueConnectorTest extends AbstractNatsToSparkTest implements Serializable {
	
	@Override
	protected NatsPublisher getNatsPublisher(final int nbOfMessages) {
		return new StandardNatsPublisher("np", NATS_SERVER_URL, DEFAULT_SUBJECT,  nbOfMessages);
	}
	
	@Test(timeout=6000)
	public void testNatsToSparkConnectorWithAdditionalPropertiesAndSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final Properties properties = new Properties();
		properties.setProperty(PROP_URL, NATS_SERVER_URL);

		final JavaPairDStream<String, String> messages = 
				NatsToSparkConnector
					.receiveFromNats(String.class, StorageLevel.MEMORY_ONLY())
					.withProperties(properties)
					.withSubjects(DEFAULT_SUBJECT)
					.asStreamOfKeyValue(ssc);

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	@Test(timeout=6000)
	public void testNatsToSparkConnectorWithAdditionalSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final JavaPairDStream<String, String> messages = 
				NatsToSparkConnector
					.receiveFromNats(String.class, StorageLevel.MEMORY_ONLY())
					.withNatsURL(NATS_SERVER_URL)
					.withSubjects(DEFAULT_SUBJECT)
					.asStreamOfKeyValue(ssc);

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	@Test(timeout=6000)
	public void testNatsToSparkConnectorWithAdditionalPropertiesAndMultipleSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final Properties properties = new Properties();
		final JavaPairDStream<String, String> messages = 
				NatsToSparkConnector
					.receiveFromNats(String.class, StorageLevel.MEMORY_ONLY())
					.withNatsURL(NATS_SERVER_URL)
					.withProperties(properties)
					.withSubjects(DEFAULT_SUBJECT, "EXTRA_SUBJECT")
					.asStreamOfKeyValue(ssc);

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	@Test(timeout=6000)
	public void testNatsToSparkConnectorWithAdditionalProperties() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final Properties properties = new Properties();
		properties.setProperty(PROP_SUBJECTS, "sub1,"+DEFAULT_SUBJECT+" , sub2");
		properties.setProperty(PROP_URL, NATS_SERVER_URL);
		final JavaPairDStream<String, String> messages = 
				NatsToSparkConnector
					.receiveFromNats(String.class, StorageLevel.MEMORY_ONLY())
					.withProperties(properties)
					.asStreamOfKeyValue(ssc);

		validateTheReceptionOfMessages(ssc, messages);
	}
}
