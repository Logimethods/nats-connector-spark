package com.logimethods.connector.spark.to_nats;

import static com.logimethods.connector.spark.to_nats.SparkToNatsConnector.combineSubjects;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Test;

public class SparkToNatsConnectorTest {

	private static final String natsURL = "nats://123.123.123.123:4444";
	private static final Properties properties = new Properties();
	private static final Collection<String> subjects = Arrays.asList("Hello", "World!");
	private static final boolean isStoredAsKeyValue = true;

	@Test
	public void testCombineSubjectsNoSubstitution() {
		final String subA = "subA";
		final String subB = "subB";

		assertEquals(subA, combineSubjects("", subA));
		assertEquals(subA+subB, combineSubjects(subA,subB));
	}

	@Test
	public void testCombineSubjectsWithSubstitution() {
		assertEquals("A.C", combineSubjects("*. =>A.", "B.C"));
		assertEquals("A.C.D", combineSubjects("*. => A.", "B.C.D"));
		assertEquals("B.C.D", combineSubjects("X.=>A.", "B.C.D"));
		assertEquals("A.D", combineSubjects("*.*.=>A.", "B.C.D"));
		assertEquals("A.B.D", combineSubjects("*.C=>A.B", "B.C.D"));
		assertTrue(SparkToNatsConnector.subjectPatternMap.toString(), SparkToNatsConnector.subjectPatternMap.containsKey("*.C=>A.B"));
		assertEquals("B.C.D", combineSubjects("*.X.*=>A.B", "B.C.D"));
		assertEquals("A.b.C.D", combineSubjects("B=>b", "A.B.C.D"));
		assertEquals("A.B.C.D", combineSubjects("^B=>b", "A.B.C.D"));
		assertEquals("A.b.B.D", combineSubjects("B=>b", "A.B.B.D"));
	}

	@Test
	// @See https://github.com/Logimethods/nats-connector-spark/pull/3
	// @See https://github.com/nats-io/java-nats-streaming/issues/51
	public void testSparkToStandardNatsConnectorImpl_Serialization() throws IOException, ClassNotFoundException {
		Long duration = 111l;
		SparkToStandardNatsConnectorImpl source = 
				new SparkToStandardNatsConnectorImpl(natsURL, properties, duration, subjects, isStoredAsKeyValue);
		
		SparkToStandardNatsConnectorImpl target = SerializationUtils.clone(source);
		
		assertEquals(source.getNatsURL(), target.getNatsURL());
		assertEquals(source.getProperties(), target.getProperties());
		assertEquals(source.getSubjects(), target.getSubjects());
		assertEquals(duration, target.connectionTimeout);
		assertEquals(isStoredAsKeyValue, target.isStoredAsKeyValue());
	}
	
}
