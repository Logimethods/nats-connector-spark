/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.to_spark;

import static com.logimethods.connector.nats.to_spark.NatsToSparkConnector.NATS_SUBJECTS;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Test;

import com.logimethods.connector.nats.spark.NatsPublisher;
import com.logimethods.connector.nats.spark.StandardNatsPublisher;
import com.logimethods.connector.nats.to_spark.NatsToSparkConnector;

public class StandardNatsToSparkConnectorTest extends AbstractNatsToSparkTest {
	
	private static final String DEFAULT_NATS_URL = null;

	@Override
	protected NatsPublisher getNatsPublisher(final int nbOfMessages) {
		return new StandardNatsPublisher("np", DEFAULT_NATS_URL, DEFAULT_SUBJECT,  nbOfMessages);
	}

	/**
	 * Test method for {@link com.logimethods.connector.nats.to_spark.NatsToSparkConnector#receiveFromNats(java.lang.String, int, java.lang.String, java.lang.String)}.
	 * @throws InterruptedException 
	 */
	@Test
	public void testNatsToSparkConnectorWithPropertiesAndSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final Properties properties = new Properties();
		final JavaReceiverInputDStream<String> messages = ssc.receiverStream(NatsToSparkConnector.receiveFromNats(properties, StorageLevel.MEMORY_ONLY(), DEFAULT_SUBJECT));

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	@Test
	public void testNatsToSparkConnectorWithAdditionalPropertiesAndSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final Properties properties = new Properties();
		final JavaReceiverInputDStream<String> messages = 
				ssc.receiverStream(NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY()).withProperties(properties).withSubjects(DEFAULT_SUBJECT));

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	@Test
	public void testNatsToSparkConnectorWithSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final JavaReceiverInputDStream<String> messages = ssc.receiverStream(NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY(), DEFAULT_SUBJECT));

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	@Test
	public void testNatsToSparkConnectorWithAdditionalSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final JavaReceiverInputDStream<String> messages = 
				ssc.receiverStream(NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY()).withSubjects(DEFAULT_SUBJECT));

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	@Test
	public void testNatsToSparkConnectorWithPropertiesAndMultipleSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final Properties properties = new Properties();
		final JavaReceiverInputDStream<String> messages = ssc.receiverStream(NatsToSparkConnector.receiveFromNats(properties, StorageLevel.MEMORY_ONLY(), DEFAULT_SUBJECT, "EXTRA_SUBJECT"));

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	@Test
	public void testNatsToSparkConnectorWithAdditionalPropertiesAndMultipleSubjects() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final Properties properties = new Properties();
		final JavaReceiverInputDStream<String> messages = 
				ssc.receiverStream(NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY()).withProperties(properties).withSubjects(DEFAULT_SUBJECT, "EXTRA_SUBJECT"));

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	/**
	 * Test method for {@link com.logimethods.connector.nats.to_spark.NatsToSparkConnector#receiveFromNats(java.lang.String, int, java.lang.String)}.
	 * @throws InterruptedException 
	 */
	@Test
	public void testNatsToSparkConnectorWithProperties() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final Properties properties = new Properties();
		properties.setProperty(NATS_SUBJECTS, "sub1,"+DEFAULT_SUBJECT+" , sub2");
		final JavaReceiverInputDStream<String> messages = ssc.receiverStream(NatsToSparkConnector.receiveFromNats(properties, StorageLevel.MEMORY_ONLY()));

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	@Test
	public void testNatsToSparkConnectorWithAdditionalProperties() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		final Properties properties = new Properties();
		properties.setProperty(NATS_SUBJECTS, "sub1,"+DEFAULT_SUBJECT+" , sub2");
		final JavaReceiverInputDStream<String> messages = ssc.receiverStream(NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY()).withProperties(properties));

		validateTheReceptionOfMessages(ssc, messages);
	}
	
	/**
	 * Test method for {@link com.logimethods.connector.nats.to_spark.NatsToSparkConnector#receiveFromNats(java.lang.String, int, java.lang.String)}.
	 * @throws Exception 
	 */
	@Test
	public void testNatsToSparkConnectorWITHOUTProperties() throws Exception {
		
		try {
			NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY()).receive();
		} catch (Exception e) {
			if (e.getMessage().contains("NatsToSparkConnector needs at least one NATS Subject"))
				return;
			else
				throw e;
		}	

		fail("An Exception(\"NatsToSparkConnector needs at least one Subject\") should have been raised.");
	}
	
	@Test
	public void testNatsToSparkConnectorWithSystemProperties() throws InterruptedException {
		
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(200));

		System.setProperty(NATS_SUBJECTS, "sub1,"+DEFAULT_SUBJECT+" , sub2");

		try {
			final JavaReceiverInputDStream<String> messages = ssc.receiverStream(NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY()));

			validateTheReceptionOfMessages(ssc, messages);
		} finally {
			System.clearProperty(NATS_SUBJECTS);
		}		
	}
}
