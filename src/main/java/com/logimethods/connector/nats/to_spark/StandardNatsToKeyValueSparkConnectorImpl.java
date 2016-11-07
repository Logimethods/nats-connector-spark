/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.to_spark;

import java.util.Collection;
import java.util.Properties;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.client.Message;
import io.nats.client.MessageHandler;
import scala.Tuple2;

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
public class StandardNatsToKeyValueSparkConnectorImpl<V> 
				extends OmnipotentStandardNatsToSparkConnector<StandardNatsToKeyValueSparkConnectorImpl<V>, Tuple2<String, V>, V> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	static final Logger logger = LoggerFactory.getLogger(StandardNatsToKeyValueSparkConnectorImpl.class);

	protected Properties enrichedProperties;

	protected StandardNatsToKeyValueSparkConnectorImpl(Class<V> type, Properties properties, StorageLevel storageLevel, String... subjects) {
		super(type, storageLevel, subjects);
		logger.debug("CREATE NatsToSparkConnector {} with Properties '{}', Storage Level {} and NATS Subjects '{}'.", this, properties, storageLevel, subjects);
	}

	protected StandardNatsToKeyValueSparkConnectorImpl(Class<V> type, StorageLevel storageLevel, String... subjects) {
		super(type, storageLevel, subjects);
		logger.debug("CREATE NatsToSparkConnector {} with Storage Level {} and NATS Subjects '{}'.", this, properties, subjects);
	}

	protected StandardNatsToKeyValueSparkConnectorImpl(Class<V> type, Properties properties, StorageLevel storageLevel) {
		super(type, storageLevel);
		logger.debug("CREATE NatsToSparkConnector {} with Properties '{}' and Storage Level {}.", this, properties, storageLevel);
	}

	protected StandardNatsToKeyValueSparkConnectorImpl(Class<V> type, StorageLevel storageLevel) {
		super(type, storageLevel);
		logger.debug("CREATE NatsToSparkConnector {}.", this, properties, storageLevel);
	}
	
	protected StandardNatsToKeyValueSparkConnectorImpl(Class<V> type, StorageLevel storageLevel, Collection<String> subjects, Properties properties, String queue, String natsUrl) {
		super(type, storageLevel, subjects, properties, queue, natsUrl);
	}
	
	/**
	@SuppressWarnings("unchecked")
	*/
	public JavaPairDStream<String, V> asStreamOf(JavaStreamingContext ssc) {
		return ssc.receiverStream(this).mapToPair(tuple -> tuple);
	}
	
	/**
	@SuppressWarnings("unchecked")
	*/
	public ReceiverInputDStream<Tuple2<String, V>> asStreamOf(StreamingContext ssc) {
		return ssc.receiverStream(this, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class));
	}

	protected MessageHandler getMessageHandler() {
		return new MessageHandler() {
			@Override
			public void onMessage(Message m) {
				final Tuple2<String, V> s = decodeTuple(m);
				
				if (logger.isTraceEnabled()) {
					logger.trace("Received by {} on Subject '{}': {}.", StandardNatsToKeyValueSparkConnectorImpl.this, m.getSubject(), s);
				}
										
				store(s);
			}
		};
	}
}

