/*******************************************************************************
 * Copyright (c) 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.spark.test;

import io.nats.client.Message;
import io.nats.client.MessageHandler;

/**
 * Simulates a simple NATS subscriber.
 */
public abstract class NatsSubscriber extends TestClient implements Runnable, MessageHandler
{
	protected String subject = null;
	protected String natsUrl;
	protected boolean checkPayload = true;

	public NatsSubscriber(String natsUrl, String id, String subject, int count)
	{
		super(id, count);
		this.natsUrl = natsUrl;
		this.subject = subject;

		logger.info("Creating NATS Subscriber ({})", id);
	}

	@Override
	public void onMessage(Message message) {

		String value = new String (message.getData());

		logger.debug("NATS Subscriber ({}):  Received message: {}", id, value);

		if (tallyMessage() == testCount)
		{
			logger.info("NATS Subscriber ({}) Received {} messages.  Completed.", id, testCount);
			setComplete();
		}
	}
}
