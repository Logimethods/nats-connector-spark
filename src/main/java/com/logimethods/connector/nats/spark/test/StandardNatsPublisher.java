/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.spark.test;

import java.time.Duration;

import io.nats.client.Nats;

public class StandardNatsPublisher extends NatsPublisher {

	public StandardNatsPublisher(String id, String natsUrl, String subject, int count) {
		super(id, natsUrl, subject, count);
	}

	@Override
	public void run() {
		try {
			logger.info("NATS Publisher ({}, {}):  Starting", natsUrl, id);

			io.nats.client.Connection c;
			try {
				c = Nats.connect(natsUrl);
			} catch (Exception e) {
				logger.error("Nats.connect({}) PRODUCES", natsUrl, e.getMessage());
				throw(e);
			}
			
			logger.debug("A NATS Connection to '{}' has been created.", c.getConnectedUrl());
			
			setReady();

			for (int i = 0; i < testCount; i++) {
				final String payload = NATS_PAYLOAD + INCR.getAndIncrement();
				c.publish(subject, payload.getBytes());
				logger.trace("Publish '{}' to '{}'.", payload, subject);
				tallyMessage();
			}
			c.flush(Duration.ofSeconds(3));

			logger.debug("NATS Publisher ({}):  Published {} messages.", id, testCount);

			setComplete();
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}

}
