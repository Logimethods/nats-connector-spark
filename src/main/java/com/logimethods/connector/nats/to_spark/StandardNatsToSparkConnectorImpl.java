/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.to_spark;

import java.io.IOException;
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
public class StandardNatsToSparkConnectorImpl extends OmnipotentStandardNatsToSparkConnector<StandardNatsToSparkConnectorImpl, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	static final Logger logger = LoggerFactory.getLogger(StandardNatsToSparkConnectorImpl.class);

	protected Properties enrichedProperties;

	protected StandardNatsToSparkConnectorImpl(Properties properties, StorageLevel storageLevel, String... subjects) {
		super(storageLevel, subjects);
		logger.debug("CREATE NatsToSparkConnector {} with Properties '{}', Storage Level {} and NATS Subjects '{}'.", this, properties, storageLevel, subjects);
	}

	protected StandardNatsToSparkConnectorImpl(StorageLevel storageLevel, String... subjects) {
		super(storageLevel, subjects);
		logger.debug("CREATE NatsToSparkConnector {} with Storage Level {} and NATS Subjects '{}'.", this, properties, subjects);
	}

	protected StandardNatsToSparkConnectorImpl(Properties properties, StorageLevel storageLevel) {
		super(storageLevel);
		logger.debug("CREATE NatsToSparkConnector {} with Properties '{}' and Storage Level {}.", this, properties, storageLevel);
	}

	protected StandardNatsToSparkConnectorImpl(StorageLevel storageLevel) {
		super(storageLevel);
		logger.debug("CREATE NatsToSparkConnector {}.", this, properties, storageLevel);
	}

	protected MessageHandler getMessageHandler() {
		return new MessageHandler() {
			@Override
			public void onMessage(Message m) {
/*					final String subject = m.getSubject();
					final String s = new String(m.getData());
					
					if (logger.isTraceEnabled()) {
						logger.trace("Received by {} on Subject '{}': {}.", StandardNatsToSparkConnectorImpl.this, m.getSubject(), s);
					}
											
					store(new Tuple2<String, String>(subject, s));
*/				
				String s = new String(m.getData());
				if (logger.isTraceEnabled()) {
					logger.trace("Received by {} on Subject '{}' sharing Queue '{}': {}.", StandardNatsToSparkConnectorImpl.this, m.getSubject(), queue, s);
				}
				store(s);
			}
		};
	}
}

