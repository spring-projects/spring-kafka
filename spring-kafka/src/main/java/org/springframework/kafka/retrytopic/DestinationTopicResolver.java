/*
 * Copyright 2016-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
