/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.spark.test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.logimethods.connector.nats_spark.IncompleteException;

import io.nats.streaming.NatsStreaming;
import io.nats.streaming.Options;
import io.nats.streaming.StreamingConnection;

public class NatsStreamingPublisher extends NatsPublisher {

	protected final String clusterID, clientID;
	
	/**
	 * @param id
	 * @param clusterID
	 * @param clientID
	 * @param natsUrl
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
			publishMessages();
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}

	/**
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws TimeoutException
	 * @throws IncompleteException 
	 */
	public void publishMessages() throws IOException, InterruptedException, TimeoutException {
		logger.debug("NATS Publisher ({}):  Starting", id);

		final Options.Builder optionsBuilder = new Options.Builder();
		if (natsUrl != null) {
			optionsBuilder.natsUrl(natsUrl);
		}
		StreamingConnection c;
		final Options options = optionsBuilder.build();
		try {
			c = NatsStreaming.connect(clusterID, clientID, options);
		} catch (Exception e) {
			logger.error("NatsStreaming.connect({}) PRODUCES {}", clusterID, clientID, ReflectionToStringBuilder.toString(options), e.getMessage());
			throw(new IOException(String.format("NatsStreaming.connect(%s, %s, %s)", clusterID, clientID, ReflectionToStringBuilder.toString(options)), e));
		}
		
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

}
