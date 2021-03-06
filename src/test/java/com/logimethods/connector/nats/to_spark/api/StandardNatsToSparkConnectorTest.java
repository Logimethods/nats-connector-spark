/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.to_spark.api;

import static com.logimethods.connector.nats.spark.test.UnitTestUtilities.NATS_URL;
import static com.logimethods.connector.nats.spark.test.UnitTestUtilities.NATS_LOCALHOST_URL;
import static com.logimethods.connector.nats_spark.Constants.PROP_SUBJECTS;
import static io.nats.client.Options.PROP_URL;

import java.util.Properties;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Test;

import com.logimethods.connector.nats.spark.test.NatsPublisher;
import com.logimethods.connector.nats.spark.test.StandardNatsPublisher;
import com.logimethods.connector.nats.to_spark.AbstractNatsToSparkTest;
import com.logimethods.connector.nats.to_spark.NatsToSparkConnector;
import com.logimethods.connector.nats.to_spark.StandardNatsToSparkConnectorImpl;

public class StandardNatsToSparkConnectorTest extends AbstractNatsToSparkTest {
	
	@Override
	protected NatsPublisher getNatsPublisher(final int nbOfMessages) {
		return new StandardNatsPublisher("np", NATS_LOCALHOST_URL, DEFAULT_SUBJECT,  nbOfMessages);
	}
	
	@Test(timeout=360000)
	public void testNatsToSparkConnectorWithAdditionalPropertiesAndSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final Properties properties = new Properties();
		properties.setProperty(PROP_URL, NATS_URL);
		final JavaReceiverInputDStream<String> messages =  
				NatsToSparkConnector
					.receiveFromNats(String.class, StorageLevel.MEMORY_ONLY())
					.withProperties(properties)
					.withSubjects(DEFAULT_SUBJECT)
					.asStreamOf(ssc);

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	@Test(timeout=360000)
	public void testNatsToSparkConnectorWithAdditionalSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final JavaReceiverInputDStream<String> messages = 
				NatsToSparkConnector
					.receiveFromNats(String.class, StorageLevel.MEMORY_ONLY())
					.withNatsURL(NATS_URL)
					.withSubjects(DEFAULT_SUBJECT)
					.asStreamOf(ssc);

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	@Test(timeout=360000)
	public void testNatsToSparkConnectorWithAdditionalPropertiesAndMultipleSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final Properties properties = new Properties();
		final JavaReceiverInputDStream<String> messages = 
				NatsToSparkConnector
					.receiveFromNats(String.class, StorageLevel.MEMORY_ONLY())
					.withNatsURL(NATS_URL)
					.withProperties(properties)
					.withSubjects(DEFAULT_SUBJECT, "EXTRA_SUBJECT")
					.asStreamOf(ssc);

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	@Test(timeout=360000)
	public void testNatsToSparkConnectorWithAdditionalProperties() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final Properties properties = new Properties();
		properties.setProperty(PROP_SUBJECTS, "sub1,"+DEFAULT_SUBJECT+" , sub2");
		properties.setProperty(PROP_URL, NATS_URL);
		final JavaReceiverInputDStream<String> messages = 
				NatsToSparkConnector
					.receiveFromNats(String.class, StorageLevel.MEMORY_ONLY())
					.withProperties(properties)
					.asStreamOf(ssc);

		validateTheReceptionOfMessages(ssc, messages);
	}
}