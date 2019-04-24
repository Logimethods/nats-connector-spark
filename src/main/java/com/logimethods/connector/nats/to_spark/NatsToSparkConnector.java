/*******************************************************************************
 * Copyright (c) 2016 Logimethods
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package com.logimethods.connector.nats.to_spark;

import static com.logimethods.connector.nats_spark.Constants.PROP_SUBJECTS;
import static io.nats.client.Options.PROP_URL;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.logimethods.connector.nats_spark.IncompleteException;
import com.logimethods.connector.nats_spark.NatsSparkUtilities;

import io.nats.client.Message;
import io.nats.streaming.StreamingConnection;
import scala.Tuple2;

/**
 * A NATS to Spark Connector.
 * <p>
 * It will transfer messages received from NATS into Spark data.
 * <p>
 * That class extends {@link org.apache.spark.streaming.receiver.Receiver}&lt;String&gt;.
 * <p>
 * An usage of this class would look like
 * <pre>
 * JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));
 * final JavaReceiverInputDStream&lt;String&gt; messages = 
 * 		NatsToSparkConnector.receiveFromNats(String.class, StorageLevel.MEMORY_ONLY(), DEFAULT_SUBJECT)).asStreamOf(ssc);
 * </pre>
 * @see <a href="https://github.com/Logimethods/nats-connector-spark">(Java based) NATS / Spark Connectors</a>
 * @see <a href="http://spark.apache.org/docs/2.0.1/streaming-custom-receivers.html">Spark Streaming Custom Receivers</a>
 * 
 * @author Laurent Magnin
 */
//@SuppressWarnings("serial")
public abstract class NatsToSparkConnector<T,R,V> extends Receiver<R> {

	private static final long serialVersionUID = -1917312737332564226L;

	static final Logger logger = LoggerFactory.getLogger(NatsToSparkConnector.class);
	
	protected final 	Class<V> type;
	protected Collection<String> subjects;
	protected Properties		 properties;
	protected String 			 natsQueue;
	protected String 			 natsUrl;
	protected Function<byte[], V> dataDecoder = null;
	protected scala.Function1<byte[], V> scalaDataDecoder = null;
	protected transient StreamingConnection connection;

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
	
	protected NatsToSparkConnector(Class<V> type, StorageLevel storageLevel, Collection<String> subjects, Properties properties, String natsQueue, String natsUrl) {
		super(storageLevel);
		this.type = type;
		this.subjects = subjects;
		this.properties = properties;
		this.natsQueue = natsQueue;
		this.natsUrl = natsUrl;
	}

	/* with... */

	/**
	 * @param subjects, the NATS Subject(s) to subscribe to
	 * @return the connector itself
	 */
	@SuppressWarnings("unchecked")
	public T withSubjects(String... subjects) {
		this.subjects = NatsSparkUtilities.transformIntoAList(subjects);
		return (T)this;
	}

	/**
	 * @param properties, the properties to set the connection to the NATS Server
	 * @return the connector itself
	 */
	@SuppressWarnings("unchecked")
	public T withProperties(Properties properties) {
		this.properties = properties;
		return (T)this;
	}

	/**
	 * @param natsURL, the NATS URL to set
	 * @return the connector itself
	 */
	@SuppressWarnings("unchecked")
	public T withNatsURL(String natsURL) {
		this.natsUrl = natsURL;
		return (T)this;
	}	
	
	/**
	 * @param queue, the NATS Queue to subscribe to
	 * @return the connector itself
	 */
	@SuppressWarnings("unchecked")
	public T withNatsQueue(String natsQueue) {
		this.natsQueue = natsQueue;
		return (T)this;
	}
	
	/**
	 * @param dataDecoder, the Data Decoder to set
	 * @return the connector itself
	 */
	@SuppressWarnings("unchecked")
	public T withDataDecoder(Function<byte[], V> dataDecoder) {
		this.dataDecoder = dataDecoder;
		return (T)this;
	}
	
	/**
	 * @param scalaDataDecoder, the Data Extractor to set
	 * @return the connector itself
	 */
	@SuppressWarnings("unchecked")
	public T withDataDecoder(scala.Function1<byte[], V> scalaDataDecoder) {
		this.scalaDataDecoder = scalaDataDecoder;
		return (T)this;
	}

	/* **************** STANDARD NATS **************** */
	
	/**
	 * Will push messages provided by NATS into a Spark Streaming.
	 *
	 * @param type, the Class of Object to expect to receive from NATS
	 * @param storageLevel, defines the StorageLevel used by Spark
	 * @return a NATS to Spark Connector.
	 */
	public static <V extends Object> StandardNatsToSparkConnectorImpl<V> receiveFromNats(Class<V> type, StorageLevel storageLevel) {
		return new StandardNatsToSparkConnectorImpl<V>(type, storageLevel);
	}

	/* **************** NATS STREAMING **************** */
	
	/**
	 * Will push messages provided by NATS into a Spark Streaming.
	 *
	 * @param type, the Class of Object to expect to receive from NATS
	 * @param storageLevel, defines the StorageLevel used by Spark
	 * @param clusterID, used by NATS Streaming
	 * @return a NATS Streaming to Spark Connector
	 */
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
		try {			
			if (connection != null) {				
				logger.info("Closing NATS Connection " + connection);
				connection.close();
				connection = null;
			}
		} catch (IOException | TimeoutException | InterruptedException e) {
			if (logger.isDebugEnabled()) {
				logger.error("Exception while unsubscribing " + e.toString());
			}
		}
	}

	/** Create a socket connection and receive data until receiver is stopped 
	 * @throws Exception
	 **/
	protected abstract void receive() throws Exception;
	
	protected void setNatsQueue() {
		if (natsQueue == null )
			natsQueue = "NatsToSparkConnector_" + NatsSparkUtilities.generateUniqueID(this) ;
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
	protected R decodeData(Message m) {
		final R s = (R) decodeData(m.getData());
		return s;
	}
	
	@SuppressWarnings("unchecked")
	protected R decodeData(io.nats.streaming.Message m) {
		final R s = (R) decodeData(m.getData());
		return s;
	}
	
	@SuppressWarnings("unchecked")
	protected R decodeTuple(Message m) {
		final String subject = m.getSubject();		
		V s = decodeData(m.getData());
		return (R) new Tuple2<String,V>(subject, s);
	}
		
	@SuppressWarnings("unchecked")
	protected R decodeTuple(io.nats.streaming.Message m) {
		final String subject = m.getSubject();		
		V s = decodeData(m.getData());
		return (R) new Tuple2<String,V>(subject, s);
	}
	
	protected V decodeData(byte[] bytes) {
		if (dataDecoder != null) {
			return dataDecoder.apply(bytes);
		} else if (scalaDataDecoder != null) {
			return scalaDataDecoder.apply(bytes);
		} else {
			return NatsSparkUtilities.decodeData(type, bytes);
		}
	}
}

