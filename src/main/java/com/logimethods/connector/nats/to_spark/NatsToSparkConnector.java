/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.to_spark;

import static com.logimethods.connector.nats_spark.Constants.PROP_SUBJECTS;
import static io.nats.client.Constants.PROP_URL;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Properties;
import java.util.function.Function;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.logimethods.connector.nats_spark.IncompleteException;
import com.logimethods.connector.nats_spark.NatsSparkUtilities;

import io.nats.client.Message;
import scala.Tuple2;

/**
 * A NATS to Spark Connector.
 * <p>
 * It will transfer messages received from NATS into Spark data.
 * <p>
 * That class extends {@link org.apache.spark.streaming.receiver.Receiver}&lt;String&gt;.
 * <p>
 * An usage of this class would look like this.
 * <pre>
 * JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));
 * final JavaReceiverInputDStream&lt;String&gt; messages = ssc.receiverStream(NatsToSparkConnector.receiveFromNats(StorageLevel.MEMORY_ONLY(), DEFAULT_SUBJECT));
 * </pre>
 * @see <a href="http://spark.apache.org/docs/1.6.1/streaming-custom-receivers.html">Spark Streaming Custom Receivers</a>
 */
@SuppressWarnings("serial")
public abstract class NatsToSparkConnector<T,R,V> extends Receiver<R> {

	static final Logger logger = LoggerFactory.getLogger(NatsToSparkConnector.class);
	
	protected final 	Class<V> type;
	protected Collection<String> subjects;
	protected Properties		 properties;
	protected String 			 queue;
	protected String 			 natsUrl;
	protected Function<byte[], V> dataExtractor = null;

	protected final static String CLIENT_ID = "NatsToSparkConnector_";

	protected NatsToSparkConnector(Class<V> type, StorageLevel storageLevel) {
		super(storageLevel);
		this.type = type;
	}

	protected NatsToSparkConnector(Class<V> type, StorageLevel storageLevel, String... subjects) {
		super(storageLevel);
		this.type = type;
		this.subjects = NatsSparkUtilities.transformIntoAList(subjects);
	}
	
	/**
	 * @param storageLevel
	 * @param subjects
	 * @param properties
	 * @param queue
	 * @param natsUrl
	 */
	protected NatsToSparkConnector(Class<V> type, StorageLevel storageLevel, Collection<String> subjects, Properties properties, String queue, String natsUrl) {
		super(storageLevel);
		this.type = type;
		this.subjects = subjects;
		this.properties = properties;
		this.queue = queue;
		this.natsUrl = natsUrl;
	}

	/* with... */

	@SuppressWarnings("unchecked")
	public T withSubjects(String... subjects) {
		this.subjects = NatsSparkUtilities.transformIntoAList(subjects);
		return (T)this;
	}

	@SuppressWarnings("unchecked")
	public T withProperties(Properties properties) {
		this.properties = properties;
		return (T)this;
	}

	/**
	 * @param natsURL the NATS URL to set
	 */
	@SuppressWarnings("unchecked")
	public T withNatsURL(String natsURL) {
		this.natsUrl = natsURL;
		return (T)this;
	}	
	
	/**
	 * @param dataExtractor the Data Extractor to set
	 */
	@SuppressWarnings("unchecked")
	public T withDataExtractor(Function<byte[], V> dataExtractor) {
		this.dataExtractor = dataExtractor;
		return (T)this;
	}

	/* **************** STANDARD NATS **************** */
	
	/**
	 * Will push into Spark Strings (messages) provided by NATS.
	 *
	 * @param storageLevel Defines the StorageLevel used by Spark.
	 * @return a NATS to Spark Connector.
	 */
	public static <V extends Object> StandardNatsToSparkConnectorImpl<V> receiveFromNats(Class<V> type, StorageLevel storageLevel) {
		return new StandardNatsToSparkConnectorImpl<V>(type, storageLevel);
	}

	/* **************** NATS STREAMING **************** */
	
	public static <V extends Object> NatsStreamingToSparkConnectorImpl<V> receiveFromNatsStreaming(Class<V> type, StorageLevel storageLevel, String clusterID) {
		return new NatsStreamingToSparkConnectorImpl<V>(type, storageLevel, clusterID, getUniqueClientName());
	}
	
	@Override
	public void onStart() {
		//Start the thread that receives data over a connection
		new Thread()  {
			@Override public void run() {
				try {
					receive();
				} catch (Exception e) {
					logger.error("Cannot start the connector: ", e);
				}
			}
		}.start();
	}

	@Override
	public void onStop() {
		// There is nothing much to do as the thread calling receive()
		// is designed to stop by itself if CTRL-C is Caught.
	}

	/** Create a socket connection and receive data until receiver is stopped 
	 * @throws Exception **/
	protected abstract void receive() throws Exception;
	
	protected void setQueue() {
		queue = "NatsToSparkConnector_" + NatsSparkUtilities.generateUniqueID(this) ;
	}

	protected Properties getProperties(){
		return properties;
	}

	protected Collection<String> getSubjects() throws IncompleteException {
		if ((subjects ==  null) || (subjects.size() == 0)) {
			final String subjectsStr = getProperties() != null ? 
											getProperties().getProperty(PROP_SUBJECTS)
											: null;
			if (subjectsStr == null) {
				throw new IncompleteException("NatsToSparkConnector needs at least one NATS Subject.");
			}
			subjects = NatsSparkUtilities.extractCollection(subjectsStr);
			logger.debug("Subject(s) provided by the Properties: '{}'", subjects);
		}
		return subjects;
	}    		

	protected String getNatsUrl() {
		if (natsUrl ==  null) {
			natsUrl = getProperties() != null ?  getProperties().getProperty(PROP_URL) : null;
			logger.debug("NatsUrl provided by the Properties: '{}'", natsUrl);
		}
		return natsUrl;
	}    		

	protected static String getUniqueClientName() {
		return CLIENT_ID + NatsSparkUtilities.generateUniqueID();
	}    
		
	@SuppressWarnings("unchecked")
	protected R extractData(Message m) {
		final R s = (R) extractData(m.getData());
		return s;
	}
	
	@SuppressWarnings("unchecked")
	protected R extractData(io.nats.stan.Message m) {
		final R s = (R) extractData(m.getData());
		return s;
	}
	
	@SuppressWarnings("unchecked")
	protected R extractTuple(Message m) {
		final String subject = m.getSubject();		
		V s = extractData(m.getData());
		return (R) new Tuple2<String,V>(subject, s);
	}
		
	@SuppressWarnings("unchecked")
	protected R extractTuple(io.nats.stan.Message m) {
		final String subject = m.getSubject();		
		V s = extractData(m.getData());
		return (R) new Tuple2<String,V>(subject, s);
	}
	
	protected V extractData(byte[] bytes) {
		if (dataExtractor != null) {
			return dataExtractor.apply(bytes);
		} else {
			return NatsSparkUtilities.extractData(type, bytes);
		}
	}
}

