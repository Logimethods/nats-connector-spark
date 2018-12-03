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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.nats.streaming.Message;
import io.nats.streaming.MessageHandler;
import io.nats.streaming.NatsStreaming;
import io.nats.streaming.Options;
import io.nats.streaming.Subscription;
import io.nats.streaming.SubscriptionOptions;

/**
 * A NATS to Spark Connector.
 * <p>
 * It will transfer messages received from NATS into Spark data.
 * <p>
 * That class extends {@link com.logimethods.connector.nats.to_spark.NatsToSparkConnector}&lt;T,R,V&gt;.
 */
public abstract class OmnipotentNatsStreamingToSparkConnector<T,R,V> extends NatsToSparkConnector<T,R,V> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	static final Logger logger = LoggerFactory.getLogger(OmnipotentNatsStreamingToSparkConnector.class);

	protected String clusterID, clientID;
	protected transient SubscriptionOptions subscriptionOpts;
	protected SubscriptionOptions.Builder subscriptionOptsBuilder;
	protected Collection<Subscription> allSubscriptions = new HashSet<Subscription>();

	/* Constructors with subjects provided by the environment */
	
	protected OmnipotentNatsStreamingToSparkConnector(Class<V> type, StorageLevel storageLevel, String clusterID, String clientID) {
		super(type, storageLevel);
		this.clusterID = clusterID;
		this.clientID = clientID;
		setNatsQueue();
//		logger.debug("CREATE NatsToSparkConnector {} with Properties '{}', Storage Level {} and NATS Subjects '{}'.", this, properties, storageLevel, subjects);
	}

	/**
	 * @param optsBuilder, the NATS Streaming options used to set the connection to NATS
	 * @return a NATS Streaming to Spark Connector
	 */
	@SuppressWarnings("unchecked")
	public T subscriptionOptionsBuilder(SubscriptionOptions.Builder optsBuilder) {
		this.subscriptionOptsBuilder = optsBuilder;
		return (T)this;
	}

    /**
     * Sets the durable subscriber name for the subscription.
     * 
     * @param durableName the name of the durable subscriber
     * @return the connector itself
     */
	@SuppressWarnings("unchecked")
    public T durableName(String durableName) {
    	getSubscriptionOptsBuilder().durableName(durableName);
    	return (T)this;
    }

    /**
     * Sets the maximum number of in-flight (unacknowledged) messages for the subscription.
     * 
     * @param maxInFlight the maximum number of in-flight messages
     * @return the connector itself
     */
	@SuppressWarnings("unchecked")
    public T maxInFlight(int maxInFlight) {
    	getSubscriptionOptsBuilder().maxInFlight(maxInFlight);
        return (T)this;
    }

    /**
     * Sets the amount of time the subscription will wait for ACKs from the cluster.
     * 
     * @param ackWait the amount of time the subscription will wait for an ACK from the cluster
     * @return the connector itself
     */
	@SuppressWarnings("unchecked")
    public T ackWait(Duration ackWait) {
    	getSubscriptionOptsBuilder().ackWait(ackWait);
        return (T)this;
    }
	
    /**
     * Sets the amount of time the subscription will wait for ACKs from the cluster.
     * 
     * @param ackWait the amount of time the subscription will wait for an ACK from the cluster
     * @param unit the time unit
     * @return the connector itself
     */
	@SuppressWarnings("unchecked")
    public T ackWait(long ackWait, TimeUnit unit) {
    	getSubscriptionOptsBuilder().ackWait(ackWait, unit);
        return (T)this;
    }

    /**
     * Sets whether or not messages must be acknowledge individually by calling
     * {@link Message#ack()}.
     * 
     * @param manualAcks whether or not messages must be manually acknowledged
     * @return the connector itself
     */
	@SuppressWarnings("unchecked")
    public T manualAcks(boolean manualAcks) {
    	if (manualAcks) getSubscriptionOptsBuilder().manualAcks();
        return (T)this;
    }

    /**
     * Specifies the sequence number from which to start receiving messages.
     * 
     * @param seq the sequence number from which to start receiving messages
     * @return the connector itself
     */
	@SuppressWarnings("unchecked")
    public T startAtSequence(long seq) {
    	getSubscriptionOptsBuilder().startAtSequence(seq);
        return (T)this;
    }

    /**
     * Specifies the desired start time position using {@code java.time.Instant}.
     * 
     * @param start the desired start time position expressed as a {@code java.time.Instant}
     * @return the connector itself
     */
	@SuppressWarnings("unchecked")
    public T startAtTime(Instant start) {
    	getSubscriptionOptsBuilder().startAtTime(start);
        return (T)this;
    }

    /**
     * Specifies the desired delta start time position in the desired unit.
     * 
     * @param ago the historical time delta (from now) from which to start receiving messages
     * @param unit the time unit
     * @return the connector itself
     */
	@SuppressWarnings("unchecked")
    public T startAtTimeDelta(long ago, TimeUnit unit) {
    	getSubscriptionOptsBuilder().startAtTimeDelta(ago, unit);
        return (T)this;
    }

    /**
     * Specifies the desired delta start time as a {@link java.time.Duration}.
     * 
     * @param ago the historical time delta (from now) from which to start receiving messages
     * @return the connector itself
     */
	@SuppressWarnings("unchecked")
    public T startAtTimeDelta(Duration ago) {
    	getSubscriptionOptsBuilder().startAtTimeDelta(ago);
        return (T)this;
    }

    /**
     * Specifies that message delivery should start with the last (most recent) message stored
     * for this subject.
     * 
     * @return the connector itself
     */
	@SuppressWarnings("unchecked")
    public T startWithLastReceived() {
    	getSubscriptionOptsBuilder().startWithLastReceived();
        return (T)this;
    }

    /**
     * Specifies that message delivery should begin at the oldest available message for this
     * subject.
     * 
     * @return the connector itself
     */
	@SuppressWarnings("unchecked")
    public T deliverAllAvailable() {
    	getSubscriptionOptsBuilder().deliverAllAvailable();
        return (T)this;
    }

	/* Deprecated methods associated with the NATS Builder */

	/**
	 * @param optsBuilder, the NATS Streaming options used to set the connection to NATS
	 * @return a NATS Streaming to Spark Connector
	 */
	@Deprecated
	public T withSubscriptionOptionsBuilder(SubscriptionOptions.Builder optsBuilder) {
		return subscriptionOptionsBuilder(optsBuilder);
	}

    /**
     * Sets the durable subscriber name for the subscription.
     * 
     * @param durableName the name of the durable subscriber
     * @return the connector itself
     */
	@Deprecated
    public T setDurableName(String durableName) {
    	return durableName(durableName);
    }
	
    /**
     * Sets the maximum number of in-flight (unacknowledged) messages for the subscription.
     * 
     * @param maxInFlight the maximum number of in-flight messages
     * @return the connector itself
     */
	@Deprecated
    public T setMaxInFlight(int maxInFlight) {
    	return maxInFlight(maxInFlight);
    }

    /**
     * Sets the amount of time the subscription will wait for ACKs from the cluster.
     * 
     * @param ackWait the amount of time the subscription will wait for an ACK from the cluster
     * @return the connector itself
     */
	@Deprecated
    public T setAckWait(Duration ackWait) {
    	return ackWait(ackWait);
    }

    /**
     * Sets the amount of time the subscription will wait for ACKs from the cluster.
     * 
     * @param ackWait the amount of time the subscription will wait for an ACK from the cluster
     * @param unit the time unit
     * @return the connector itself
     */
	@Deprecated
    public T setAckWait(long ackWait, TimeUnit unit) {
    	return ackWait(ackWait, unit);
    }

    /**
     * Sets whether or not messages must be acknowledge individually by calling
     * {@link Message#ack()}.
     * 
     * @param manualAcks whether or not messages must be manually acknowledged
     * @return the connector itself
     */
	@Deprecated
    public T setManualAcks(boolean manualAcks) {
    	return manualAcks(manualAcks);
    }
	
	/* End of the deprecated methods associated with the NATS Builder */
	
	/**
	 * @return the opts
	 */
	protected SubscriptionOptions getSubscriptionOptions() {
		if ((subscriptionOpts == null) && (subscriptionOptsBuilder != null)){
			subscriptionOpts = subscriptionOptsBuilder.build();
		}
		return subscriptionOpts;
	}

	/**
	 * @return the optsBuilder
	 */
	protected SubscriptionOptions.Builder getSubscriptionOptsBuilder() {
		if (subscriptionOptsBuilder == null) {
			subscriptionOptsBuilder = new SubscriptionOptions.Builder();
		}
		return subscriptionOptsBuilder;
	}

	/**
	 * @return a NATS Streaming to Spark Connector where the NATS Messages are stored in Spark as Key (the NATS Subject) / Value (the NATS Payload)
	 */
	public NatsStreamingToKeyValueSparkConnectorImpl<V> storedAsKeyValue() {
		return new NatsStreamingToKeyValueSparkConnectorImpl<V>(type, storageLevel(), subjects, properties, natsQueue, natsUrl, clusterID, clientID, 
																subscriptionOpts, subscriptionOptsBuilder, dataDecoder, scalaDataDecoder);
	}

	/** Create a socket connection and receive data until receiver is stopped 
	 * @throws Exception **/
	protected void receive() throws Exception {

		// Make connection and initialize streams			  
		final Options.Builder optionsBuilder = new Options.Builder();
		if (natsUrl != null) {
			optionsBuilder.natsUrl(natsUrl);
		}

		connection = NatsStreaming.connect(clusterID, clientID, optionsBuilder.build());

//		logger.info("A NATS from '{}' to Spark Connection has been created for '{}', sharing Queue '{}'.", connection.getConnectedUrl(), this, queue);
		
		for (String subject: getSubjects()) {
			final Subscription sub = connection.subscribe(subject, natsQueue, getMessageHandler(), getSubscriptionOptions());
			allSubscriptions.add(sub);
			
			logger.info("Listening on {}.", subject);
			
			Runtime.getRuntime().addShutdownHook(new Thread(new Runnable(){
				@Override
				public void run() {
					logger.debug("Caught CTRL-C, shutting down gracefully..." + this);
					
					try {
						allSubscriptions.remove(sub);
						if (keepConnectionDurable()) {
							logger.info("Closing NATS Subscription at Shutdown " + sub);
							sub.close();
						} else {
							logger.info("Unsubscribing NATS Connection at Shutdown " + sub);
							sub.unsubscribe();
						}							
					} catch (IOException | IllegalStateException e) {
						if (logger.isDebugEnabled()) {
							logger.error("Exception while unsubscribing at Shutdown " + e.toString());
						}
					}
					
					try {
						if ((! keepConnectionDurable()) && (connection != null)) {
							logger.info("Closing NATS Connection at Shutdown " + connection);
							connection.close();
						}
						connection = null;
					} catch (IOException | TimeoutException | InterruptedException e) {
						if (logger.isDebugEnabled()) {
							logger.error("Exception while unsubscribing at Shutdown " + e.toString());
						}
					}
				}
			}));
		}
	}
	
	@Override
	public void onStop() {
		try {			
			Iterator<Subscription> setIterator = allSubscriptions.iterator();
			while (setIterator.hasNext()) {
				final Subscription sub = setIterator.next();
				try {
					if (keepConnectionDurable()) {
						logger.info("Closing NATS Subscription to keep it DURABLE: " + sub);
						sub.close();
					} else {
						logger.info("Unsubscribing NATS Connection " + sub);
						sub.unsubscribe();
					}							
				} catch (IOException e) {
					if (logger.isDebugEnabled()) {
						logger.error("Exception while unsubscribing " + e.toString());
					}
				}
			    setIterator.remove();
			}

			if ((! keepConnectionDurable()) && (connection != null)) {				
				logger.info("Closing NATS Connection to keep it DURABLE: " + connection);
				connection.close();
				connection = null;
			}
		} catch (IOException | TimeoutException | InterruptedException e) {
			if (logger.isDebugEnabled()) {
				logger.error("Exception while unsubscribing " + e.toString());
			}
		}
	}

	protected boolean keepConnectionDurable() {
		final String durableName = getSubscriptionOptsBuilder().build().getDurableName();
		return (durableName != null && !durableName.isEmpty());
	}

	abstract protected MessageHandler getMessageHandler();
}

