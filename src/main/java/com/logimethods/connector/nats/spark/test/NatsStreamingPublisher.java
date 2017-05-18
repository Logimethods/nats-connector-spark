/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.spark.test;

import io.nats.streaming.NatsStreaming;
import io.nats.streaming.Options;
import io.nats.streaming.StreamingConnection;

public class NatsStreamingPublisher extends NatsPublisher {

	protected final String clusterID, clientID;
	
	/**
	 * @param clusterID
	 * @param clientID
	 * @param natsUrl
	 * @param id
	 * @param subject
	 * @param count
	 */
	public NatsStreamingPublisher(String id, String clusterID, String clientID, String natsUrl, String subject, int count) {
		super(id, natsUrl, subject, count);
		this.clusterID = clusterID;
		this.clientID = clientID;
	}

	@Override
	public void run() {

		try {

			logger.debug("NATS Publisher ({}):  Starting", id);

			final Options.Builder optionsBuilder = new Options.Builder();
			if (natsUrl != null) {
				optionsBuilder.natsUrl(natsUrl);
			}
			final StreamingConnection c = NatsStreaming.connect(clusterID, clientID, optionsBuilder.build());
			
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
