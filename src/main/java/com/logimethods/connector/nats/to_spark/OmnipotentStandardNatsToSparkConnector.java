/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.to_spark;

import static io.nats.client.Options.PROP_URL;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.spark.storage.StorageLevel;

import com.logimethods.connector.nats_spark.IncompleteException;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.MessageHandler;
import io.nats.client.Nats;
import io.nats.client.Options;

/**
 * A NATS to Spark Connector.
 * <p>
 * It will transfer messages received from NATS into Spark data.
 * <p>
 * That class extends {@link com.logimethods.connector.nats.to_spark.NatsToSparkConnector}&lt;T,R,V&gt;.
 */
@SuppressWarnings("serial")
public abstract class OmnipotentStandardNatsToSparkConnector<T,R,V> extends NatsToSparkConnector<T,R,V> {
			
	protected OmnipotentStandardNatsToSparkConnector(Class<V> type, Properties properties, StorageLevel storageLevel, String... subjects) {
		super(type, storageLevel, subjects);
		this.properties = properties;
		setNatsQueue();
	}

	protected OmnipotentStandardNatsToSparkConnector(Class<V> type, StorageLevel storageLevel, String... subjects) {
		super(type, storageLevel, subjects);
		setNatsQueue();
	}

	protected OmnipotentStandardNatsToSparkConnector(Class<V> type, Properties properties, StorageLevel storageLevel) {
		super(type, storageLevel);
		this.properties = properties;
		setNatsQueue();
	}

	protected OmnipotentStandardNatsToSparkConnector(Class<V> type, StorageLevel storageLevel) {
		super(type, storageLevel);
		setNatsQueue();
	}
	
	protected OmnipotentStandardNatsToSparkConnector(Class<V> type, StorageLevel storageLevel, Collection<String> subjects, Properties properties, String queue, String natsUrl) {
		super(type, storageLevel, subjects, properties, queue, natsUrl);
	}

	/**
	 */
	protected StandardNatsToKeyValueSparkConnectorImpl<V> storedAsKeyValue() {
		return new StandardNatsToKeyValueSparkConnectorImpl<V>(type, storageLevel(), subjects, properties, natsQueue, natsUrl, dataDecoder, scalaDataDecoder);
	}

	protected Properties enrichedProperties;

	/** Create a socket connection and receive data until receiver is stopped 
	 * @throws IncompleteException 
	 * @throws TimeoutException 
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws IllegalArgumentException 
	 * @throws IllegalStateException **/
	protected void receive() throws IncompleteException, IOException, TimeoutException, IllegalStateException, IllegalArgumentException, InterruptedException {

		// Make connection and initialize streams			  
		Connection connection;
		final Options options = new Options.Builder(getEnrichedProperties()).build();
		try {
			connection = Nats.connect(options);
		} catch (Exception e) {
			logger.error("Nats.connect({}, {}, {}) PRODUCES {}", ReflectionToStringBuilder.toString(options), e.getMessage());
			throw(e);
		}
		logger.info("A NATS from '{}' to Spark Connection has been created for '{}', sharing Queue '{}'.", connection.getConnectedUrl(), this, natsQueue);

		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable(){
			@Override
			public void run() {
				logger.debug("Caught CTRL-C, shutting down gracefully..." + connection);
				try {
					Thread.sleep(500); // To allow the `dispatcher.unsubscribe(subject);` to be call
					if (connection != null ) connection.close();
				} catch (InterruptedException e) {
					logger.debug(e.getMessage());
				}
			}
		}));

		for (String subject: getSubjects()) {
			final Dispatcher dispatcher = connection.createDispatcher(getMessageHandler()).subscribe(subject, natsQueue);
			logger.info("Listening on {}.", subject);
			
			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable(){
				@Override
				public void run() {
					try {
						logger.debug("Caught CTRL-C, shutting down gracefully..." + dispatcher);
						dispatcher.unsubscribe(subject);
						connection.closeDispatcher(dispatcher);
					} catch (IllegalStateException e) {
						if (logger.isDebugEnabled()) {
							logger.error("Exception while unsubscribing " + e.toString());
						}
					}
				}
			}));
		}
	}

	protected Properties getEnrichedProperties() throws IncompleteException {
		if (enrichedProperties == null) {
			enrichedProperties = getProperties();
			if (enrichedProperties == null) {
				enrichedProperties = new Properties();
			}
			if (natsUrl != null) {
				enrichedProperties.setProperty(PROP_URL, natsUrl);
			}
		}
		return enrichedProperties;
	}

	abstract protected MessageHandler getMessageHandler();
}

