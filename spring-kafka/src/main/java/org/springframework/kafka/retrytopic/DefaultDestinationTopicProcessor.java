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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 *
 * Default implementation of the {@link DestinationTopicProcessor} interface.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.7
 *
 */
public class DefaultDestinationTopicProcessor implements DestinationTopicProcessor {

	private final DestinationTopicResolver destinationTopicResolver;

	public DefaultDestinationTopicProcessor(DestinationTopicResolver destinationTopicResolver) {
		this.destinationTopicResolver = destinationTopicResolver;
	}

	@Override
	public void processDestinationTopicProperties(Consumer<DestinationTopic.Properties> destinationPropertiesProcessor,
												Context context) {
		context
				.properties
				.stream()
				.forEach(destinationPropertiesProcessor);
	}

	@Override
	public void registerDestinationTopic(String mainTopicName, String destinationTopicName,
										DestinationTopic.Properties destinationTopicProperties, Context context) {
		List<DestinationTopic> topicDestinations = context.destinationsByTopicMap
				.computeIfAbsent(mainTopicName, newTopic -> new ArrayList<>());
		topicDestinations.add(new DestinationTopic(destinationTopicName, destinationTopicProperties));
	}

	@Override
	public void processRegisteredDestinations(Consumer<Collection<String>> topicsCallback, Context context) {
		context
				.destinationsByTopicMap
				.values()
				.forEach(topicDestinations -> this.destinationTopicResolver.addDestinationTopics(
						context.listenerId, topicDestinations));
		topicsCallback.accept(getAllTopicsNamesForThis(context));
	}

	private List<String> getAllTopicsNamesForThis(Context context) {
		return context.destinationsByTopicMap
				.values()
				.stream()
				.flatMap(Collection::stream)
				.map(DestinationTopic::getDestinationName)
				.collect(Collectors.toList());
	}
}
