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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.BDDMockito.then;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * @author Tomaz Fernandes
 * @since 2.7
 */
@ExtendWith(MockitoExtension.class)
class DefaultDestinationTopicProcessorTests extends DestinationTopicTests {

	@Mock
	private DestinationTopicResolver destinationTopicResolver;

	@Captor
	private ArgumentCaptor<Map<String, DestinationTopicResolver.DestinationsHolder>> destinationMapCaptor;

	private final DefaultDestinationTopicProcessor destinationTopicProcessor =
			new DefaultDestinationTopicProcessor(destinationTopicResolver);

	@Test
	void shouldProcessDestinationProperties() {
		// setup
		DestinationTopicProcessor.Context context = new DestinationTopicProcessor.Context(allProps);
		List<DestinationTopic.Properties> processedProps = new ArrayList<>();

		// when
		destinationTopicProcessor.processDestinationProperties(props -> processedProps.add(props), context);

		// then
		assertEquals(allProps, processedProps);
	}

	@Test
	void shouldRegisterTopicDestinations() {
		// setup
		DestinationTopicProcessor.Context context = new DestinationTopicProcessor.Context(allProps);

		// when
		registerFirstTopicDestinations(context);
		registerSecondTopicDestinations(context);
		registerThirdTopicDestinations(context);

		// then
		assertTrue(context.destinationsByTopicMap.containsKey(FIRST_TOPIC));
		List<DestinationTopic> destinationTopicsForFirstTopic = context.destinationsByTopicMap.get(FIRST_TOPIC);
		assertEquals(4, destinationTopicsForFirstTopic.size());
		assertEquals(mainDestinationTopic, destinationTopicsForFirstTopic.get(0));
		assertEquals(firstRetryDestinationTopic, destinationTopicsForFirstTopic.get(1));
		assertEquals(secondRetryDestinationTopic, destinationTopicsForFirstTopic.get(2));
		assertEquals(dltDestinationTopic, destinationTopicsForFirstTopic.get(3));

		assertTrue(context.destinationsByTopicMap.containsKey(SECOND_TOPIC));
		List<DestinationTopic> destinationTopicsForSecondTopic = context.destinationsByTopicMap.get(SECOND_TOPIC);
		assertEquals(4, destinationTopicsForSecondTopic.size());
		assertEquals(mainDestinationTopic2, destinationTopicsForSecondTopic.get(0));
		assertEquals(firstRetryDestinationTopic2, destinationTopicsForSecondTopic.get(1));
		assertEquals(secondRetryDestinationTopic2, destinationTopicsForSecondTopic.get(2));
		assertEquals(dltDestinationTopic2, destinationTopicsForSecondTopic.get(3));

		assertTrue(context.destinationsByTopicMap.containsKey(THIRD_TOPIC));
		List<DestinationTopic> destinationTopicsForThirdTopic = context.destinationsByTopicMap.get(THIRD_TOPIC);
		assertEquals(3, destinationTopicsForThirdTopic.size());
		assertEquals(mainDestinationTopic3, destinationTopicsForThirdTopic.get(0));
		assertEquals(firstRetryDestinationTopic3, destinationTopicsForThirdTopic.get(1));
		assertEquals(secondRetryDestinationTopic3, destinationTopicsForThirdTopic.get(2));
	}

	private void registerFirstTopicDestinations(DestinationTopicProcessor.Context context) {
		allFirstDestinationsHolders.forEach(propsHolder ->
				destinationTopicProcessor.registerDestinationTopic(FIRST_TOPIC,
						getSuffixedName(propsHolder), propsHolder.props, context));
	}

	private String getSuffixedName(PropsHolder propsHolder) {
		return propsHolder.topicName + propsHolder.props.suffix();
	}

	private void registerSecondTopicDestinations(DestinationTopicProcessor.Context context) {
		allSecondDestinationHolders.forEach(propsHolder ->
				destinationTopicProcessor.registerDestinationTopic(SECOND_TOPIC,
						getSuffixedName(propsHolder), propsHolder.props, context));
	}

	private void registerThirdTopicDestinations(DestinationTopicProcessor.Context context) {
		allThirdDestinationHolders.forEach(propsHolder ->
				destinationTopicProcessor.registerDestinationTopic(THIRD_TOPIC,
						getSuffixedName(propsHolder), propsHolder.props, context));
	}

	@Test
	void shouldCreateDestinationMapWhenProcessDestinations() {
		// setup
		DefaultDestinationTopicProcessor destinationTopicProcessor =
				new DefaultDestinationTopicProcessor(destinationTopicResolver);

		DestinationTopicProcessor.Context context = new DestinationTopicProcessor.Context(allProps);

		// when
		registerFirstTopicDestinations(context);
		registerSecondTopicDestinations(context);
		registerThirdTopicDestinations(context);
		destinationTopicProcessor.processRegisteredDestinations(topic -> { }, context);

		// then
		then(destinationTopicResolver).should().addDestinations(destinationMapCaptor.capture());
		Map<String, DestinationTopicResolver.DestinationsHolder> destinationMap = destinationMapCaptor.getValue();

		assertEquals(11, destinationMap.size());

		assertTrue(destinationMap.containsKey(mainDestinationTopic.getDestinationName()));
		assertEquals(mainDestinationTopic, destinationMap.get(mainDestinationTopic.getDestinationName()).getSourceDestination());
		assertEquals(firstRetryDestinationTopic, destinationMap.get(mainDestinationTopic.getDestinationName()).getNextDestination());
		assertTrue(destinationMap.containsKey(firstRetryDestinationTopic.getDestinationName()));
		assertEquals(firstRetryDestinationTopic, destinationMap.get(firstRetryDestinationTopic.getDestinationName()).getSourceDestination());
		assertEquals(secondRetryDestinationTopic, destinationMap.get(firstRetryDestinationTopic.getDestinationName()).getNextDestination());
		assertTrue(destinationMap.containsKey(secondRetryDestinationTopic.getDestinationName()));
		assertEquals(secondRetryDestinationTopic, destinationMap.get(secondRetryDestinationTopic.getDestinationName()).getSourceDestination());
		assertEquals(dltDestinationTopic, destinationMap.get(secondRetryDestinationTopic.getDestinationName()).getNextDestination());
		assertTrue(destinationMap.containsKey(dltDestinationTopic.getDestinationName()));
		assertEquals(dltDestinationTopic, destinationMap.get(dltDestinationTopic.getDestinationName()).getSourceDestination());
		assertEquals(noOpsDestinationTopic, destinationMap.get(dltDestinationTopic.getDestinationName()).getNextDestination());

		assertTrue(destinationMap.containsKey(mainDestinationTopic2.getDestinationName()));
		assertEquals(mainDestinationTopic2, destinationMap.get(mainDestinationTopic2.getDestinationName()).getSourceDestination());
		assertEquals(firstRetryDestinationTopic2, destinationMap.get(mainDestinationTopic2.getDestinationName()).getNextDestination());
		assertTrue(destinationMap.containsKey(firstRetryDestinationTopic2.getDestinationName()));
		assertEquals(firstRetryDestinationTopic2, destinationMap.get(firstRetryDestinationTopic2.getDestinationName()).getSourceDestination());
		assertEquals(secondRetryDestinationTopic2, destinationMap.get(firstRetryDestinationTopic2.getDestinationName()).getNextDestination());
		assertTrue(destinationMap.containsKey(secondRetryDestinationTopic2.getDestinationName()));
		assertEquals(secondRetryDestinationTopic2, destinationMap.get(secondRetryDestinationTopic2.getDestinationName()).getSourceDestination());
		assertEquals(dltDestinationTopic2, destinationMap.get(secondRetryDestinationTopic2.getDestinationName()).getNextDestination());
		assertTrue(destinationMap.containsKey(dltDestinationTopic2.getDestinationName()));
		assertEquals(dltDestinationTopic2, destinationMap.get(dltDestinationTopic2.getDestinationName()).getSourceDestination());
		assertEquals(noOpsDestinationTopic2, destinationMap.get(dltDestinationTopic2.getDestinationName()).getNextDestination());

		assertTrue(destinationMap.containsKey(mainDestinationTopic3.getDestinationName()));
		assertEquals(mainDestinationTopic3, destinationMap.get(mainDestinationTopic3.getDestinationName()).getSourceDestination());
		assertEquals(firstRetryDestinationTopic3, destinationMap.get(mainDestinationTopic3.getDestinationName()).getNextDestination());
		assertTrue(destinationMap.containsKey(firstRetryDestinationTopic3.getDestinationName()));
		assertEquals(firstRetryDestinationTopic3, destinationMap.get(firstRetryDestinationTopic3.getDestinationName()).getSourceDestination());
		assertEquals(secondRetryDestinationTopic3, destinationMap.get(firstRetryDestinationTopic3.getDestinationName()).getNextDestination());
		assertTrue(destinationMap.containsKey(secondRetryDestinationTopic3.getDestinationName()));
		assertEquals(secondRetryDestinationTopic3, destinationMap.get(secondRetryDestinationTopic3.getDestinationName()).getSourceDestination());
		assertEquals(noOpsDestinationTopic3, destinationMap.get(secondRetryDestinationTopic3.getDestinationName()).getNextDestination());
	}

	@Test
	void shouldApplyTopicsCallback() {
		// setup
		DefaultDestinationTopicProcessor destinationTopicProcessor =
				new DefaultDestinationTopicProcessor(destinationTopicResolver);

		DestinationTopicProcessor.Context context = new DestinationTopicProcessor.Context(allProps);

		List<String> allTopics = allFirstDestinationsTopics
				.stream()
				.map(destinationTopic -> destinationTopic.getDestinationName())
				.collect(Collectors.toList());

		allTopics.addAll(allSecondDestinationTopics
				.stream()
				.map(destinationTopic -> destinationTopic.getDestinationName())
				.collect(Collectors.toList()));

		List<String> allProcessedTopics = new ArrayList<>();

		// when
		registerFirstTopicDestinations(context);
		registerSecondTopicDestinations(context);
		destinationTopicProcessor.processRegisteredDestinations(topics -> allProcessedTopics.addAll(topics), context);

		// then
		assertEquals(allTopics, allProcessedTopics);

	}
}
