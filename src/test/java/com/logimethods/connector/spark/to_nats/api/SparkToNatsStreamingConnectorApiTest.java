package com.logimethods.connector.spark.to_nats.api;

import java.time.Duration;

import com.logimethods.connector.spark.to_nats.SparkToNatsConnectorPool;

public class SparkToNatsStreamingConnectorApiTest {
	
//	@Test
	public void checkAPIisPublic() {	
		SparkToNatsConnectorPool
			.newStreamingPool("clusterID")
			.withProperties(null)
			.withConnectionTimeout(Duration.ofSeconds(2))
			.withSubjects("subject2")
			.publishToNats(null);
	}

}
