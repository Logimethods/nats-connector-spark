package com.logimethods.connector.nats.to_spark;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.apache.spark.storage.StorageLevel;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class NatsToSparkConnectorTest {
    @Rule
    public ExpectedException thrown= ExpectedException.none();

	@Test
	public void testExtractDataByteArray_String() {
		StandardNatsToSparkConnectorImpl<String> connector = 
				NatsToSparkConnector
					.receiveFromNats(String.class, StorageLevel.MEMORY_ONLY());
		
		String str = "A small piece of Text!";
		byte[] bytes = str.getBytes();
		assertEquals(str, connector.extractData(bytes));
	}

	@Test
	public void testExtractDataByteArray_Float() {
		StandardNatsToSparkConnectorImpl<Float> connector = 
				NatsToSparkConnector
					.receiveFromNats(Float.class, StorageLevel.MEMORY_ONLY());
		
		Float f = 1234324234.34f;
		byte[] bytes = ByteBuffer.allocate(Float.BYTES).putFloat(f).array();
		assertEquals(f, connector.extractData(bytes));
	}

	@Test
	public void testExtractDataByteArray_Exception() {		
		thrown.expect(UnsupportedOperationException.class);
		
		StandardNatsToSparkConnectorImpl<NatsToSparkConnectorTest> connector = 
				NatsToSparkConnector
					.receiveFromNats(NatsToSparkConnectorTest.class, StorageLevel.MEMORY_ONLY());
		
		byte[] bytes = "xxxx".getBytes();
		connector.extractData(bytes);
	}

}
