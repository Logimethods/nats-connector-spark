package com.logimethods.connector.nats.to_spark;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.function.Function;

import org.apache.spark.storage.StorageLevel;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.logimethods.connector.nats_spark.NatsSparkUtilities;

import io.nats.streaming.SubscriptionOptions;

@SuppressWarnings("serial")
public class NatsToSparkConnectorTest implements Serializable {
    private static final String DURABLE_NAME = "DURABLE_NAME";
    
	@Rule
    public ExpectedException thrown= ExpectedException.none();

	@Test
	public void testExtractDataByteArray_String() {
		StandardNatsToSparkConnectorImpl<String> connector = 
				NatsToSparkConnector
					.receiveFromNats(String.class, StorageLevel.MEMORY_ONLY());
		
		String str = "A small piece of Text!";
		byte[] bytes = NatsSparkUtilities.encodeData(str);
		assertEquals(str, connector.decodeData(bytes));
	}

	@Test
	public void testExtractDataByteArray_Float() {
		StandardNatsToSparkConnectorImpl<Float> connector = 
				NatsToSparkConnector
					.receiveFromNats(Float.class, StorageLevel.MEMORY_ONLY());
		
		Float f = 1234324234.34f;
		byte[] bytes = NatsSparkUtilities.encodeData(f);
		assertEquals(f, connector.decodeData(bytes));
	}

	@Test
	public void testPublicExtractDataByteArray_Float() {
		Float f = 1234324234.34f;
		byte[] bytes = NatsSparkUtilities.encodeData(f);
		assertEquals(f, NatsSparkUtilities.decodeData(float.class, bytes));
	}

	@Test
	public void testExtractDataByteArray_Exception() {		
		thrown.expect(UnsupportedOperationException.class);
		
		StandardNatsToSparkConnectorImpl<NatsToSparkConnectorTest> connector = 
				NatsToSparkConnector
					.receiveFromNats(NatsToSparkConnectorTest.class, StorageLevel.MEMORY_ONLY());
		
		byte[] bytes = "xxxx".getBytes();
		connector.decodeData(bytes);
	}

	@Test
	public void testExtractDataByteArray_DataExtractor() throws IOException {
		final Function<byte[], Dummy> dataExtractor = bytes -> {
			ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
			ObjectInput in = null;
			Object o = null;
			try {
				try {
					in = new ObjectInputStream(bis);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				o = in.readObject();
			} catch (ClassNotFoundException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				try {
					if (in != null) {
						in.close();
					}
				} catch (IOException ex) {
					// ignore close exception
				}
			}
			return (Dummy) o;

		};
		StandardNatsToSparkConnectorImpl<Dummy> connector = 
				NatsToSparkConnector
					.receiveFromNats(Dummy.class, StorageLevel.MEMORY_ONLY())
					.withDataDecoder(dataExtractor);

		Dummy dummy = new Dummy("Name");
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = new ObjectOutputStream(bos);   
		out.writeObject(dummy);
		out.flush();
		byte[] bytes = bos.toByteArray();
		bos.close();
		
		assertEquals(dummy, connector.decodeData(bytes));
	}
	
	@Test
	// @See https://github.com/Logimethods/nats-connector-spark/pull/3
	// @See https://github.com/nats-io/java-nats-streaming/issues/51
	public void testSubscriptionOptions_BuilderSerialization() throws IOException, ClassNotFoundException {
		SubscriptionOptions.Builder optsBuilder = new SubscriptionOptions.Builder().setDurableName(DURABLE_NAME);

		final SubscriptionOptions.Builder newOptsBuilder = (SubscriptionOptions.Builder) serializeDeserialize(optsBuilder);
		
		assertEquals(DURABLE_NAME, newOptsBuilder.build().getDurableName());
	}

	@Test
	// @See https://github.com/Logimethods/nats-connector-spark/pull/3
	// @See https://github.com/nats-io/java-nats-streaming/issues/51
	public void testNatsStreamingToSparkConnectorImpl_Serialization() throws IOException, ClassNotFoundException {
		SubscriptionOptions.Builder optsBuilder = new SubscriptionOptions.Builder().setDurableName(DURABLE_NAME);
		final NatsStreamingToSparkConnectorImpl<String> connector = 
				NatsToSparkConnector
				.receiveFromNatsStreaming(String.class, StorageLevel.MEMORY_ONLY(), "clusterID") 
				.withSubscriptionOptionsBuilder(optsBuilder)
				.deliverAllAvailable() 
				.withNatsURL("NATS_URL") 
				.withSubjects("DEFAULT_SUBJECT");
	
		@SuppressWarnings("unchecked")
		final NatsStreamingToSparkConnectorImpl<String> newConnector = (NatsStreamingToSparkConnectorImpl<String>) serializeDeserialize(connector);
		
		assertEquals(DURABLE_NAME, newConnector.getSubscriptionOptions().getDurableName());
	}

	protected Object serializeDeserialize(Object object)
			throws IOException, ClassNotFoundException {
		byte[] bytes = null;
		ByteArrayOutputStream bos = null;
		ObjectOutputStream oos = null;
		bos = new ByteArrayOutputStream();
		oos = new ObjectOutputStream(bos);
		oos.writeObject(object);
		oos.flush();
		bytes = bos.toByteArray();
		oos.close();
		bos.close();

		Object obj = null;
		ByteArrayInputStream bis = null;
		ObjectInputStream ois = null;
		bis = new ByteArrayInputStream(bytes);
		ois = new ObjectInputStream(bis);
		obj = ois.readObject();
		bis.close();
		ois.close();
		return obj;
	}
}

