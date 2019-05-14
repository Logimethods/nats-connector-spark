/*******************************************************************************
 * Copyright (c) 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.spark.test;

import io.nats.client.Dispatcher;
import io.nats.client.Nats;

public class StandardNatsSubscriber extends NatsSubscriber {

	/**
	 * @param natsUrl
	 * @param id
	 * @param subject
	 * @param count
	 */
	public StandardNatsSubscriber(String natsUrl, String id, String subject, int count) {
		super(natsUrl, id, subject, count);
	}

	@Override
	public void run() {

		try {
			logger.info("NATS Subscriber ({}):  Subscribing to subject: {}", id, subject); //trace

			io.nats.client.Connection c;
			try {
				c = Nats.connect(natsUrl);
			} catch (Exception e) {
				logger.error("Nats.connect({}) PRODUCES", natsUrl, e.getMessage());
				throw(e);
			}
			final Dispatcher dispatcher = c.createDispatcher(this).subscribe(subject);

			setReady();

			logger.info("NATS Subscriber ({}):  Subscribing to subject: {}", id, subject); // debug

			waitForCompletion();

			dispatcher.unsubscribe(subject);

			logger.info("NATS Subscriber ({}):  Exiting.", id); // debug
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}

}
