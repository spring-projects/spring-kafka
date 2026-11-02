/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.kafka.retrytopic;

/**
 *
 * Provides methods for resolving the destination to which a message that failed
 * to be processed should be forwarded to.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.7
 *
 */
public interface DestinationTopicResolver extends DestinationTopicContainer {

	/**
	 *
	 * Resolves the destination topic for the failed message.
	 * @param mainListenerId the listener id.
	 * @param topic the current topic for the message.
	 * @param attempt the number of processing attempts already made for that message.
	 * @param e the exception the message processing has thrown
	 * @param originalTimestamp the time when the first attempt to process the message
	 *                             threw an exception.
	 * @return the {@link DestinationTopic} for the given parameters.
	 *
	 */
	DestinationTopic resolveDestinationTopic(String mainListenerId, String topic, Integer attempt, Exception e,
			long originalTimestamp);

}
