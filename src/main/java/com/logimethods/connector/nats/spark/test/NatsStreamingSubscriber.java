/*******************************************************************************
 * Copyright (c) 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.spark.test;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.logimethods.connector.nats_spark.NatsSparkUtilities;

import io.nats.streaming.Message;
import io.nats.streaming.MessageHandler;
import io.nats.streaming.NatsStreaming;
import io.nats.streaming.Options;
import io.nats.streaming.StreamingConnection;
import io.nats.streaming.Subscription;

public class NatsStreamingSubscriber<V> extends NatsSubscriber {

	private String clusterName;
	private String clientName;
	private Class<V> type;
	private Collection<V> data;

	/**
	 * @param natsUrl
	 * @param id
	 * @param subject
	 * @param clusterName
	 * @param clientName
	 * @param data
	 * @param type
	 */
	public NatsStreamingSubscriber(String natsUrl, String id, String subject, String clusterName, String clientName, Collection<V> data, Class<V> type) {
		super(natsUrl, id, subject, data.size());
		this.type = type;
		this.clusterName = clusterName;
		this.clientName = clientName;
		this.data = new LinkedList<V>(data);
	}

	@Override
	public void run() {
		try {
			logger.info("NATS Subscriber ({}):  Subscribing to subject: {}", id, subject); //trace

			final Options.Builder optionsBuilder = new Options.Builder();
			if (natsUrl != null) {
				optionsBuilder.natsUrl(natsUrl);
			}
			StreamingConnection c;
			final Options options = optionsBuilder.build();
			try {
				c = NatsStreaming.connect(clusterName, clientName, options);
			} catch (Exception e) {
				logger.error("NatsStreaming.connect({}, {}, {}) PRODUCES {}", clusterName, clientName, ReflectionToStringBuilder.toString(options), e.getMessage());
				throw(e);
			}
			
//				AsyncSubscription s = c.subscribeAsync(subject, this);
//				s.start();
			Subscription sub = c.subscribe(subject, new MessageHandler() {
			    public void onMessage(Message m) {
					final int tallyMessage = tallyMessage();
					logger.info("NATS Subscriber ({}) Received {}/{} message(s).", clientName, tallyMessage, testCount);

					//System.out.printf("Received a message: %s\n", m.getData());
			    	final V obj = NatsSparkUtilities.decodeData(type, m.getData());
			    	logger.info("Received a message ({}) on subject: {}", obj, subject);
/*			        if (! data.remove(obj)) {
			        	throw new RuntimeException(data.toString() + " does not contain " + obj);
			        }*/
			        
					if (tallyMessage == testCount)
					{
						logger.info("NATS Subscriber ({}) Received {} messages.  Completed.", clientName, testCount);
						setComplete();
					}
			    }
			});

			setReady();

			logger.info("NATS Subscriber waiting for Completion"); // debug

			waitForCompletion();

			// Unsubscribe
			sub.unsubscribe();

			// Close connection
			c.close();
			
			logger.info("NATS Subscriber ({}):  Exiting.", id); // debug
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}

	public String getClusterName() {
		return clusterName;
	}

	public String getClientName() {
		return clientName;
	}

	public Collection<V> getData() {
		return data;
	}

}
