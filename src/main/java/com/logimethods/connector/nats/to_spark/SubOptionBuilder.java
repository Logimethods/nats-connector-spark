package com.logimethods.connector.nats.to_spark;

import io.nats.stan.Message;
import io.nats.stan.SubscriptionOptions;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Mapping for @{link io.nats.stan.SubscriptionOptions.Builder}.
 * 
 * @author dungcv - [cdung.vo@gmail.com]
 */
public class SubOptionBuilder implements Serializable {

	private static final long serialVersionUID = 5807259822230294889L;

	private enum Position {First, Last} 

	private String durableName;
	private Integer maxInFlight;
	private Duration ackWait;
	private Boolean manualAcks;
	private Long startSequence;
	private Instant startTime;
	private Position position;

	/**
	 * Default Constructor.
	 */
	public SubOptionBuilder() {
		durableName = null;
		maxInFlight = null;
		ackWait = null;
		manualAcks = null;
		startSequence = null;
		startTime = null;
		position = null;
	}

	/**
     * Sets the durable subscriber name for the subscription.
     * 
     * @param durableName the name of the durable subscriber
     */
    public void setDurableName(String durableName) {
    	this.durableName = durableName;
    }

    /**
     * Sets the maximum number of in-flight (unacknowledged) messages for the subscription.
     * 
     * @param maxInFlight the maximum number of in-flight messages
     */
    public void setMaxInFlight(int maxInFlight) {
    	this.maxInFlight = maxInFlight;
    }

    /**
     * Sets the amount of time the subscription will wait for ACKs from the cluster.
     * 
     * @param ackWait the amount of time the subscription will wait for an ACK from the cluster
     */
    public void setAckWait(Duration ackWait) {
    	this.ackWait = ackWait;
    }

    /**
     * Sets the amount of time the subscription will wait for ACKs from the cluster.
     * 
     * @param ackWait the amount of time the subscription will wait for an ACK from the cluster
     * @param unit the time unit
     */
    public void setAckWait(long ackWait, TimeUnit unit) {
    	this.ackWait = Duration.ofMillis(unit.toMillis(ackWait));
    }

    /**
     * Sets whether or not messages must be acknowledge individually by calling
     * {@link Message#ack()}.
     * 
     * @param manualAcks whether or not messages must be manually acknowledged
     */
    public void setManualAcks(boolean manualAcks) {
    	this.manualAcks = manualAcks;
    }

    /**
     * Specifies the sequence number from which to start receiving messages.
     * 
     * @param seq the sequence number from which to start receiving messages
     */
    public void startAtSequence(long seq) {
    	this.position = null;
    	this.startTime = null;
    	this.startSequence = seq;
    }

    /**
     * Specifies the desired start time position using {@code java.time.Instant}.
     * 
     * @param start the desired start time position expressed as a {@code java.time.Instant}
     */
    public void startAtTime(Instant start) {
    	this.position = null;
    	this.startSequence = null;
    	this.startTime = start;
    }

    /**
     * Specifies the desired delta start time position in the desired unit.
     * 
     * @param ago the historical time delta (from now) from which to start receiving messages
     * @param unit the time unit
     */
    public void startAtTimeDelta(long ago, TimeUnit unit) {
    	startAtTime(Instant.now().minusNanos(unit.toNanos(ago)));
    }

    /**
     * Specifies the desired delta start time as a {@link java.time.Duration}.
     * 
     * @param ago the historical time delta (from now) from which to start receiving messages
     */
    public void startAtTimeDelta(Duration ago) {
    	startAtTime(Instant.now().minusNanos(ago.toNanos()));
    }

    /**
     * Specifies that message delivery should start with the last (most recent) message stored
     * for this subject.
     */
    public void startWithLastReceived() {
    	position = Position.Last;
    }

    /**
     * Specifies that message delivery should begin at the oldest available message for this
     * subject.
     */
    public void deliverAllAvailable() {
    	position = Position.First;
    }

    /**
     * Build @{link io.nats.stan.SubscriptionOptions} from this.
     * @return
     */
    public SubscriptionOptions build() {
    	SubscriptionOptions.Builder builder = new SubscriptionOptions.Builder();
    	if (durableName != null) {
    		builder.setDurableName(durableName);
    	}
    	if (maxInFlight != null) {
    		builder.setMaxInFlight(maxInFlight);
    	}
    	if(ackWait != null) {
    		builder.setAckWait(ackWait);
    	}
    	if (manualAcks != null) {
    		builder.setManualAcks(manualAcks);
    	}
    	if (startSequence != null) {
    		builder.startAtSequence(startSequence);
    	}
    	if (startTime != null) {
    		builder.startAtTime(startTime);
    	}
    	if (position != null) {
    		if (position == Position.First) {
    			builder.deliverAllAvailable();
    		} else {
    			builder.startWithLastReceived();
    		}
    	}
        return builder.build();
    }
}
