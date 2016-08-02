/*******************************************************************************
 * Copyright (c) 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.nats.connector.spark;

//import io.nats.stan.AsyncSubscription;
import io.nats.stan.ConnectionFactory;
import io.nats.stan.Message;
import io.nats.stan.MessageHandler;
import io.nats.stan.Subscription;

public class StreamingNatsSubscriber extends NatsSubscriber {

	private String clusterName;
	private String clientName;

	/**
	 * @param id
	 * @param subject
	 * @param count
	 */
	public StreamingNatsSubscriber(String id, String subject, String clusterName, String clientName, int count) {
		super(id, subject, count);
		this.clusterName = clusterName;
		this.clientName = clientName;
	}

	@Override
	public void run() {

		try {
			logger.info("NATS Subscriber ({}):  Subscribing to subject: {}", id, subject); //trace

			io.nats.stan.Connection c = new ConnectionFactory(clusterName, clientName).createConnection();

//			AsyncSubscription s = c.subscribeAsync(subject, this);
//			s.start();
			Subscription sub = c.subscribe(subject, new MessageHandler() {
			    public void onMessage(Message m) {
			        //System.out.printf("Received a message: %s\n", m.getData());
			        logger.info("Received a message ({}) on subject: {}", m.getData(), subject);
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
