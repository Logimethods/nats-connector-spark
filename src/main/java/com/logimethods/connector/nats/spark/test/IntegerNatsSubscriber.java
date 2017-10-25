/*******************************************************************************
 * Copyright (c) 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.spark.test;

import io.nats.client.AsyncSubscription;
import io.nats.client.ConnectionFactory;
import io.nats.client.Message;

import java.nio.ByteBuffer;

public class IntegerNatsSubscriber extends NatsSubscriber {

	/**
	 * @param id
	 * @param subject
	 * @param count
	 */
	public IntegerNatsSubscriber(String natsUrl, String id, String subject, int count) {
		super(natsUrl, id, subject, count);
	}

	@Override
	public void run() {

		try {
			logger.info("NATS Subscriber ({}):  Subscribing to subject: {}", id, subject); //trace

			io.nats.client.Connection c = new ConnectionFactory(natsUrl).createConnection();

			AsyncSubscription s = c.subscribeAsync(subject, this);
			s.start();

			setReady();

			logger.info("NATS Subscriber ({}):  Subscribing to subject: {}", id, subject); // debug

			waitForCompletion();

			s.unsubscribe();

			logger.info("NATS Subscriber ({}):  Exiting.", id); // debug
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}

	@Override
	public void onMessage(Message message) {

		Integer value = ByteBuffer.wrap(message.getData()).getInt();

		logger.debug("NATS Subscriber ({}):  Received message: {}", id, value);

		if (tallyMessage() == testCount)
		{
			logger.info("NATS Subscriber ({}) Received {} messages.  Completed.", id, testCount);
			setComplete();
		}
	}
}
