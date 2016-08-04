/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.nats.connector.spark;

import io.nats.stan.Connection;
import io.nats.stan.ConnectionFactory;

public class StreamingNatsPublisher extends NatsPublisher {

	protected final String clusterID, clientID;
	
	/**
	 * @param clusterID
	 * @param clientID
	 * @param natsUrl
	 * @param id
	 * @param subject
	 * @param count
	 */
	public StreamingNatsPublisher(String id, String clusterID, String clientID, String natsUrl, String subject, int count) {
		super(id, natsUrl, subject, count);
		this.clusterID = clusterID;
		this.clientID = clientID;
	}

	@Override
	public void run() {

		try {

			logger.debug("NATS Publisher ({}):  Starting", id);

			final ConnectionFactory cf = new ConnectionFactory(clusterID, clientID);
			if (natsUrl != null) {
				cf.setNatsUrl(natsUrl);
			}
			Connection c = cf.createConnection();
			
			logger.debug("A NATS Connection to '{}' has been created.", c);
			
			setReady();

			for (int i = 0; i < testCount; i++) {
				final String payload = NATS_PAYLOAD + INCR.getAndIncrement();
				c.publish(subject, payload.getBytes());
				logger.trace("Publish '{}' to '{}'.", payload, subject);
				tallyMessage();
			}

			logger.debug("NATS Publisher ({}):  Published {} messages.", id, testCount);

			setComplete();
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}

}
