/*
 * Copyright 2018-2021 the original author or authors.
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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 *
 * The {@link DestinationTopicProcessor} creates and registers the
 * {@link DestinationTopic} instances in the provided {@link Context}, also
 * providing callback interfaces to be called upon the context properties.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public interface DestinationTopicProcessor {

	/**
	 * Process the destination properties.
	 * @param destinationPropertiesProcessor the processor.
	 * @param context the context.
	 */
	void processDestinationProperties(Consumer<DestinationTopic.Properties> destinationPropertiesProcessor, Context context);

	/**
	 * Process the registered destinations.
	 * @param topicsConsumer the consumer.
	 * @param context the context.
	 */
	void processRegisteredDestinations(Consumer<Collection<String>> topicsConsumer, Context context);

	/**
	 * Register the destination topic.
	 * @param mainTopicName the main topic name.
	 * @param destinationTopicName the destination topic name.
	 * @param destinationTopicProperties the destination topic properties.
	 * @param context the context.
	 */
	void registerDestinationTopic(String mainTopicName, String destinationTopicName,
			DestinationTopic.Properties destinationTopicProperties, Context context);

	class Context {

		protected final Map<String, List<DestinationTopic>> destinationsByTopicMap; // NOSONAR

		protected final List<DestinationTopic.Properties> properties; // NOSONAR

		public Context(List<DestinationTopic.Properties> properties) {

			this.destinationsByTopicMap = new HashMap<>();

			this.properties = properties;
		}
	}

}
