package com.logimethods.connector.spark.to_nats;

import static org.junit.Assert.*;
import static com.logimethods.connector.spark.to_nats.SparkToNatsConnector.combineSubjects;

import org.junit.Test;

public class SparkToNatsConnectorTest {

	@Test
	public void testCombineSubjectsNoSubstitution() {
		final String subA = "subA";
		final String subB = "subB";

		assertEquals(subA, combineSubjects("", subA));
		assertEquals(subA+subB, combineSubjects(subA,subB));
	}

	@Test
	public void testCombineSubjectsWithSubstitution() {
		assertEquals("A.C", combineSubjects("*.:A.", "B.C"));
		assertEquals("A.C.D", combineSubjects("*.:A.", "B.C.D"));
		assertEquals("B.C.D", combineSubjects("X.:A.", "B.C.D"));
		assertEquals("A.D", combineSubjects("*.*.:A.", "B.C.D"));
		assertEquals("A.B.D", combineSubjects("*.C:A.B", "B.C.D"));
		assertTrue(SparkToNatsConnector.subjectPatternMap.toString(), SparkToNatsConnector.subjectPatternMap.containsKey("*.C:A.B"));
		assertEquals("B.C.D", combineSubjects("*.X.*:A.B", "B.C.D"));
		assertEquals("A.b.C.D", combineSubjects("B:b", "A.B.C.D"));
		assertEquals("A.B.C.D", combineSubjects("^B:b", "A.B.C.D"));
		assertEquals("A.b.B.D", combineSubjects("B:b", "A.B.B.D"));
	}

}
