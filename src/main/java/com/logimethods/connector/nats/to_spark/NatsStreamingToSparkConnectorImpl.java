/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.to_spark;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.stan.Connection;
import io.nats.stan.ConnectionFactory;
import io.nats.stan.Message;
import io.nats.stan.MessageHandler;
import io.nats.stan.Subscription;
import io.nats.stan.SubscriptionOptions;

import java.nio.ByteBuffer;

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
public class NatsStreamingToSparkConnectorImpl extends NatsToSparkConnector<NatsStreamingToSparkConnectorImpl, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	static final Logger logger = LoggerFactory.getLogger(NatsStreamingToSparkConnectorImpl.class);

	protected String clusterID, clientID;
	protected SubscriptionOptions opts;
	protected SubscriptionOptions.Builder optsBuilder;

	/* Constructors with subjects provided by the environment */
	
	protected NatsStreamingToSparkConnectorImpl(StorageLevel storageLevel, String clusterID, String clientID) {
		super(storageLevel);
		this.clusterID = clusterID;
		this.clientID = clientID;
		setQueue();
//		logger.debug("CREATE NatsToSparkConnector {} with Properties '{}', Storage Level {} and NATS Subjects '{}'.", this, properties, storageLevel, subjects);
	}

	public NatsStreamingToSparkConnectorImpl withSubscriptionOptionsBuilder(SubscriptionOptions.Builder optsBuilder) {
		this.optsBuilder = optsBuilder;
		return this;
	}

    /**
     * Sets the durable subscriber name for the subscription.
     * 
     * @param durableName the name of the durable subscriber
     * @return this
     */
    public NatsStreamingToSparkConnectorImpl setDurableName(String durableName) {
    	getOptsBuilder().setDurableName(durableName);
    	return this;
    }

    /**
     * Sets the maximum number of in-flight (unacknowledged) messages for the subscription.
     * 
     * @param maxInFlight the maximum number of in-flight messages
     * @return this
     */
    public NatsStreamingToSparkConnectorImpl setMaxInFlight(int maxInFlight) {
    	getOptsBuilder().setMaxInFlight(maxInFlight);
        return this;
    }

    /**
     * Sets the amount of time the subscription will wait for ACKs from the cluster.
     * 
     * @param ackWait the amount of time the subscription will wait for an ACK from the cluster
     * @return this
     */
    public NatsStreamingToSparkConnectorImpl setAckWait(Duration ackWait) {
    	getOptsBuilder().setAckWait(ackWait);
        return this;
    }

    /**
     * Sets the amount of time the subscription will wait for ACKs from the cluster.
     * 
     * @param ackWait the amount of time the subscription will wait for an ACK from the cluster
     * @param unit the time unit
     * @return this
     */
    public NatsStreamingToSparkConnectorImpl setAckWait(long ackWait, TimeUnit unit) {
    	getOptsBuilder().setAckWait(ackWait, unit);
        return this;
    }

    /**
     * Sets whether or not messages must be acknowledge individually by calling
     * {@link Message#ack()}.
     * 
     * @param manualAcks whether or not messages must be manually acknowledged
     * @return this
     */
    public NatsStreamingToSparkConnectorImpl setManualAcks(boolean manualAcks) {
    	getOptsBuilder().setManualAcks(manualAcks);
        return this;
    }

    /**
     * Specifies the sequence number from which to start receiving messages.
     * 
     * @param seq the sequence number from which to start receiving messages
     * @return this
     */
    public NatsStreamingToSparkConnectorImpl startAtSequence(long seq) {
    	getOptsBuilder().startAtSequence(seq);
        return this;
    }

    /**
     * Specifies the desired start time position using {@code java.time.Instant}.
     * 
     * @param start the desired start time position expressed as a {@code java.time.Instant}
     * @return this
     */
    public NatsStreamingToSparkConnectorImpl startAtTime(Instant start) {
    	getOptsBuilder().startAtTime(start);
        return this;
    }

    /**
     * Specifies the desired delta start time position in the desired unit.
     * 
     * @param ago the historical time delta (from now) from which to start receiving messages
     * @param unit the time unit
     * @return this
     */
    public NatsStreamingToSparkConnectorImpl startAtTimeDelta(long ago, TimeUnit unit) {
    	getOptsBuilder().startAtTimeDelta(ago, unit);
        return this;
    }

    /**
     * Specifies the desired delta start time as a {@link java.time.Duration}.
     * 
     * @param ago the historical time delta (from now) from which to start receiving messages
     * @return this
     */
    public NatsStreamingToSparkConnectorImpl startAtTimeDelta(Duration ago) {
    	getOptsBuilder().startAtTimeDelta(ago);
        return this;
    }

    /**
     * Specifies that message delivery should start with the last (most recent) message stored
     * for this subject.
     * 
     * @return this
     */
    public NatsStreamingToSparkConnectorImpl startWithLastReceived() {
    	getOptsBuilder().startWithLastReceived();
        return this;
    }

    /**
     * Specifies that message delivery should begin at the oldest available message for this
     * subject.
     * 
     * @return this
     */
    public NatsStreamingToSparkConnectorImpl deliverAllAvailable() {
    	getOptsBuilder().deliverAllAvailable();
        return this;
    }

	
	/**
	 * @return the opts
	 */
	protected SubscriptionOptions getSubscriptionOptions() {
		if ((opts == null) && (optsBuilder != null)){
			opts = optsBuilder.build();
		}
		return opts;
	}

	/**
	 * @return the optsBuilder
	 */
	protected SubscriptionOptions.Builder getOptsBuilder() {
		if (optsBuilder == null) {
			optsBuilder = new SubscriptionOptions.Builder();
		}
		return optsBuilder;
	}

	/** Create a socket connection and receive data until receiver is stopped 
	 * @throws Exception **/
	protected void receive() throws Exception {

		// Make connection and initialize streams			  
		final ConnectionFactory connectionFactory = new ConnectionFactory(clusterID, clientID);
		if (getNatsUrl() != null) {
			connectionFactory.setNatsUrl(getNatsUrl());
		}
		final Connection connection = connectionFactory.createConnection();
//		logger.info("A NATS from '{}' to Spark Connection has been created for '{}', sharing Queue '{}'.", connection.getConnectedUrl(), this, queue);
		
		for (String subject: getSubjects()) {
			final Subscription sub = connection.subscribe(subject, queue, new MessageHandler() {
				@Override
				public void onMessage(Message m) {
					if (subjectAndPayload) {
						final String subject = m.getSubject();
						final byte[] payload = m.getData();
						final ByteBuffer bytes = ByteBuffer.wrap(payload);
						
						if (logger.isTraceEnabled()) {
							logger.trace("Received by {} on Subject '{}': {}.", NatsStreamingToSparkConnectorImpl.this,
									m.getSubject(), payload.toString());
						}
						store(bytes, subject);
					} else {
						String s = new String(m.getData());
						if (logger.isTraceEnabled()) {
							logger.trace("Received by {} on Subject '{}': {}.", NatsStreamingToSparkConnectorImpl.this,
									m.getSubject(), s);
						}
						store(s);
					}
				}
			}, getSubscriptionOptions());
			logger.info("Listening on {}.", subject);
			
			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable(){
				@Override
				public void run() {
					logger.debug("Caught CTRL-C, shutting down gracefully..." + this);
					try {
						sub.unsubscribe();
					} catch (IOException | TimeoutException e) {
						if (logger.isDebugEnabled()) {
							logger.error("Exception while unsubscribing " + e.toString());
						}
					}
					try {
						connection.close();
					} catch (IOException | TimeoutException e) {
						if (logger.isDebugEnabled()) {
							logger.error("Exception while unsubscribing " + e.toString());
						}
					}
				}
			}));
		}
	}
}

