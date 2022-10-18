/*
 * Copyright 2017-2022 the original author or authors.
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

import java.util.List;

import org.springframework.lang.Nullable;

/**
 *
 * Provides methods to store and retrieve {@link DestinationTopic} instances.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.7
 */
public interface DestinationTopicContainer {

	/**
	 * Adds the provided destination topics to the container.
	 * @param mainListenerId the listener id.
	 * @param destinationTopics the {@link DestinationTopic} list to add.
	 */
	void addDestinationTopics(String mainListenerId, List<DestinationTopic> destinationTopics);

	/**
	 * Returns the {@link DestinationTopic} instance registered for that topic.
	 * @param mainListenerId the listener id.
	 * @param topicName the topic name of the DestinationTopic to be returned.
	 * @return the DestinationTopic instance registered for that topic.
	 */
	DestinationTopic getDestinationTopicByName(String mainListenerId, String topicName);

	/**
	 * Returns the {@link DestinationTopic} instance registered as the next
	 * destination topic in the chain for the given topic.
	 * Note that this might not correspond to the actual next topic a message will
	 * be forwarded to, since that depends on different factors.
	 *
	 * If you need to find out the exact next topic for a message use the
	 * {@link DestinationTopicResolver#resolveDestinationTopic(String, Integer, Exception, long)}
	 * method instead.
	 * @param mainListenerId the listener id.
	 * @param topicName the topic name of the DestinationTopic to be returned.
	 * @return the next DestinationTopic in the chain registered for that topic.
	 */
	DestinationTopic getNextDestinationTopicFor(String mainListenerId, String topicName);

	/**
	 * Returns the {@link DestinationTopic} instance registered as
	 * DLT for the given topic, or null if none is found.
	 * @param mainListenerId the listener id.
	 * @param topicName the topic name for which to look the DLT for
	 * @return The {@link DestinationTopic} instance corresponding to the DLT.
	 */
	@Nullable
	DestinationTopic getDltFor(String mainListenerId, String topicName);

}
