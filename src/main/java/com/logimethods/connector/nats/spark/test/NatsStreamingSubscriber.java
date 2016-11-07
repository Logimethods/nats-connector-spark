/*******************************************************************************
 * Copyright (c) 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.spark.test;

import java.util.Collection;
import java.util.LinkedList;

import com.logimethods.connector.nats_spark.NatsSparkUtilities;

import io.nats.stan.ConnectionFactory;
import io.nats.stan.Message;
import io.nats.stan.MessageHandler;
import io.nats.stan.Subscription;

public class NatsStreamingSubscriber<V> extends NatsSubscriber {

	private String clusterName;
	private String clientName;
	private Class<V> type;
	private Collection<V> data;

	/**
	 * @param id
	 * @param subject
	 * @param count
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

        	final ConnectionFactory connectionFactory = new ConnectionFactory(clusterName, clientName);
        	if (natsUrl != null) {
        		connectionFactory.setNatsUrl(natsUrl);
        	}

			io.nats.stan.Connection c = connectionFactory.createConnection();

//			AsyncSubscription s = c.subscribeAsync(subject, this);
//			s.start();
			Subscription sub = c.subscribe(subject, new MessageHandler() {
			    public void onMessage(Message m) {
			        //System.out.printf("Received a message: %s\n", m.getData());
			    	final V obj = NatsSparkUtilities.decodeData(type, m.getData());
			    	logger.info("Received a message ({}) on subject: {}", obj, subject);
			        if (! data.remove(obj)) {
			        	throw new RuntimeException(data.toString() + " does not contain " + obj);
			        }
					if (tallyMessage() == testCount)
					{
						logger.info("NATS Subscriber ({}) Received {} messages.  Completed.", clientName, testCount);
						setComplete();
					}
			    }
			});

			setReady();

			logger.info("NATS Subscriber ({}):  Subscribing to subject: {}", id, subject); // debug

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

}
