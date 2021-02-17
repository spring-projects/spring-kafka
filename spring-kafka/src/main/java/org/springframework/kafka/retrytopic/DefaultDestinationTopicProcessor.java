/*
 * Copyright 2017-2021 the original author or authors.
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
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 *
 * Default implementation of the {@link DestinationTopicProcessor} interface.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public class DefaultDestinationTopicProcessor implements DestinationTopicProcessor {

	private static final String NO_OPS_SUFFIX = "-noOps";

	private final DestinationTopicResolver destinationTopicResolver;

	public DefaultDestinationTopicProcessor(DestinationTopicResolver destinationTopicResolver) {
		this.destinationTopicResolver = destinationTopicResolver;
	}

	@Override
	public void processDestinationProperties(Consumer<DestinationTopic.Properties> destinationPropertiesProcessor,
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
		Map<String, DestinationTopicResolver.DestinationsHolder> sourceDestinationMapForThisInstance = context.destinationsByTopicMap
				.values()
				.stream()
				.map(this::correlatePairSourceAndDestinationValues)
				.reduce(this::concatenateMaps)
				.orElseThrow(() -> new IllegalStateException("No destinations where provided for the Retry Topic configuration"));
		this.destinationTopicResolver.addDestinations(sourceDestinationMapForThisInstance);
		topicsCallback.accept(getAllTopicsNamesForThis(context));
	}

	private Map<String, DestinationTopicResolver.DestinationsHolder> concatenateMaps(
												Map<String, DestinationTopicResolver.DestinationsHolder> firstMap,
												Map<String, DestinationTopicResolver.DestinationsHolder> secondMap) {
		firstMap.putAll(secondMap);
		return firstMap;
	}

	private Map<String, DestinationTopicResolver.DestinationsHolder> correlatePairSourceAndDestinationValues(
												List<DestinationTopic> destinationList) {
		return IntStream
				.range(0, destinationList.size())
				.boxed()
				.collect(Collectors.toMap(index -> destinationList.get(index).getDestinationName(),
						index -> DestinationTopicResolver.holderFor(destinationList.get(index),
								getNextDestinationTopic(destinationList, index))));
	}

	private DestinationTopic getNextDestinationTopic(List<DestinationTopic> destinationList, int index) {
		return index != destinationList.size() - 1
				? destinationList.get(index + 1)
				: new DestinationTopic(destinationList.get(index).getDestinationName() + NO_OPS_SUFFIX,
				destinationList.get(index), NO_OPS_SUFFIX, DestinationTopic.Type.NO_OPS);
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
