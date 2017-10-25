/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.spark.test;

import java.nio.ByteBuffer;

import io.nats.client.ConnectionFactory;

public class IntegerNatsPublisher extends NatsPublisher {

	public IntegerNatsPublisher(String id, String natsUrl, String subject, int count) {
		super(id, natsUrl, subject, count);
	}

	@Override
	public void run() {

		try {

			logger.debug("NATS Publisher ({}):  Starting", id);

			ConnectionFactory cf = new ConnectionFactory(natsUrl);
			io.nats.client.Connection c = cf.createConnection();
			
			logger.debug("A NATS Connection to '{}' has been created.", c.getConnectedUrl());
			
			setReady();

			for (int i = 0; i < testCount; i++) {
				final ByteBuffer buffer = ByteBuffer.allocate(4);
				final int payload = NATS_PAYLOAD_INT + INCR.getAndIncrement();
				buffer.putInt(payload);				
				c.publish(subject, buffer.array());
				logger.trace("Publish '{}' to '{}'.", payload, subject);
				tallyMessage();
			}
			c.flush();

			logger.debug("NATS Publisher ({}):  Published {} messages.", id, testCount);

			setComplete();
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}

}
