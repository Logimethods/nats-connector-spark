/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.to_spark;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.logimethods.connector.nats_spark.IncompleteException;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.Subscription;
import scala.Tuple2;

import static io.nats.client.Constants.*;

/**
 * A NATS to Spark Connector.
 * <p>
 * It will transfer messages received from NATS into Spark data.
 * <p>
 * That class extends {@link org.apache.spark.streaming.receiver.Receiver}&lt;String&gt;.
 * <p>
 * An usage of this class would look like this.
 * <pre>
 * JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));
 * final JavaReceiverInputDStream&lt;String&gt; messages = ssc.receiverStream(NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY(), DEFAULT_SUBJECT));
 * </pre>
 * @see <a href="http://spark.apache.org/docs/1.6.2/streaming-custom-receivers.html">Spark Streaming Custom Receivers</a>
 */
@SuppressWarnings("serial")
public abstract class OmnipotentStandardNatsToSparkConnector<T,R> extends NatsToSparkConnector<T,R> {

	protected OmnipotentStandardNatsToSparkConnector(Properties properties, StorageLevel storageLevel, String... subjects) {
		super(storageLevel, subjects);
		this.properties = properties;
		setQueue();
	}

	protected OmnipotentStandardNatsToSparkConnector(StorageLevel storageLevel, String... subjects) {
		super(storageLevel, subjects);
		setQueue();
	}

	protected OmnipotentStandardNatsToSparkConnector(Properties properties, StorageLevel storageLevel) {
		super(storageLevel);
		this.properties = properties;
		setQueue();
	}

	protected OmnipotentStandardNatsToSparkConnector(StorageLevel storageLevel) {
		super(storageLevel);
		setQueue();
	}
	
	protected OmnipotentStandardNatsToSparkConnector(StorageLevel storageLevel, Collection<String> subjects, Properties properties, String queue, String natsUrl) {
		super(storageLevel, subjects, properties, queue, natsUrl);
	}

	/**
	 */
	@SuppressWarnings("unchecked")
	public T storedAsKeyValue() {
		return (T) new StandardNatsToKeyValueSparkConnectorImpl(storageLevel(), subjects, properties, queue, natsUrl);
	}

	protected Properties enrichedProperties;

	/** Create a socket connection and receive data until receiver is stopped 
	 * @throws IncompleteException 
	 * @throws TimeoutException 
	 * @throws IOException 
	 * @throws Exception **/
	protected void receive() throws IncompleteException, IOException, TimeoutException {

		// Make connection and initialize streams			  
		final ConnectionFactory connectionFactory = new ConnectionFactory(getEnrichedProperties());
		final Connection connection = connectionFactory.createConnection();
		logger.info("A NATS from '{}' to Spark Connection has been created for '{}', sharing Queue '{}'.", connection.getConnectedUrl(), this, queue);
		
		for (String subject: getSubjects()) {
			final Subscription sub = connection.subscribe(subject, queue, getMessageHandler());
			logger.info("Listening on {}.", subject);
			
			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable(){
				@Override
				public void run() {
					logger.debug("Caught CTRL-C, shutting down gracefully..." + this);
					try {
						sub.unsubscribe();
					} catch (IOException e) {
						if (logger.isDebugEnabled()) {
							logger.error("Exception while unsubscribing " + e.toString());
						}
					}
					connection.close();
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

