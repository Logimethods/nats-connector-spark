/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.nats.connector.spark;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simulates a simple NATS publisher.
 */
public abstract class NatsPublisher extends TestClient implements Runnable
{
	public static final String NATS_PAYLOAD = "Hello from NATS! ";
	protected static final AtomicInteger INCR = new AtomicInteger();

	String subject;
	String natsUrl;

	public NatsPublisher(String id, String natsUrl, String subject, int count)
	{
		super(id, count);
		this.subject = subject;
		this.natsUrl = natsUrl;

		logger.debug("Creating NATS Publisher ({})", id);
	}
}
