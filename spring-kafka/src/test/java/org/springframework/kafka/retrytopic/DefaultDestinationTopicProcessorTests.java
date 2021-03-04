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

import static org.assertj.core.api.Assertions.assertThat;
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
		destinationTopicProcessor.processDestinationTopicProperties(props -> processedProps.add(props), context);

		// then
		assertThat(processedProps).isEqualTo(allProps);
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
		assertThat(context.destinationsByTopicMap.containsKey(FIRST_TOPIC)).isTrue();
		List<DestinationTopic> destinationTopicsForFirstTopic = context.destinationsByTopicMap.get(FIRST_TOPIC);
		assertThat(destinationTopicsForFirstTopic.size()).isEqualTo(4);
		assertThat(destinationTopicsForFirstTopic.get(0)).isEqualTo(mainDestinationTopic);
		assertThat(destinationTopicsForFirstTopic.get(1)).isEqualTo(firstRetryDestinationTopic);
		assertThat(destinationTopicsForFirstTopic.get(2)).isEqualTo(secondRetryDestinationTopic);
		assertThat(destinationTopicsForFirstTopic.get(3)).isEqualTo(dltDestinationTopic);

		assertThat(context.destinationsByTopicMap.containsKey(SECOND_TOPIC)).isTrue();
		List<DestinationTopic> destinationTopicsForSecondTopic = context.destinationsByTopicMap.get(SECOND_TOPIC);
		assertThat(destinationTopicsForSecondTopic.size()).isEqualTo(4);
		assertThat(destinationTopicsForSecondTopic.get(0)).isEqualTo(mainDestinationTopic2);
		assertThat(destinationTopicsForSecondTopic.get(1)).isEqualTo(firstRetryDestinationTopic2);
		assertThat(destinationTopicsForSecondTopic.get(2)).isEqualTo(secondRetryDestinationTopic2);
		assertThat(destinationTopicsForSecondTopic.get(3)).isEqualTo(dltDestinationTopic2);

		assertThat(context.destinationsByTopicMap.containsKey(THIRD_TOPIC)).isTrue();
		List<DestinationTopic> destinationTopicsForThirdTopic = context.destinationsByTopicMap.get(THIRD_TOPIC);
		assertThat(destinationTopicsForThirdTopic.size()).isEqualTo(3);
		assertThat(destinationTopicsForThirdTopic.get(0)).isEqualTo(mainDestinationTopic3);
		assertThat(destinationTopicsForThirdTopic.get(1)).isEqualTo(firstRetryDestinationTopic3);
		assertThat(destinationTopicsForThirdTopic.get(2)).isEqualTo(secondRetryDestinationTopic3);
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

		assertThat(destinationMap.size()).isEqualTo(11);

		assertThat(destinationMap.containsKey(mainDestinationTopic.getDestinationName())).isTrue();
		assertThat(destinationMap.get(mainDestinationTopic.getDestinationName()).getSourceDestination()).isEqualTo(mainDestinationTopic);
		assertThat(destinationMap.get(mainDestinationTopic.getDestinationName()).getNextDestination()).isEqualTo(firstRetryDestinationTopic);
		assertThat(destinationMap.containsKey(firstRetryDestinationTopic.getDestinationName())).isTrue();
		assertThat(destinationMap.get(firstRetryDestinationTopic.getDestinationName()).getSourceDestination()).isEqualTo(firstRetryDestinationTopic);
		assertThat(destinationMap.get(firstRetryDestinationTopic.getDestinationName()).getNextDestination()).isEqualTo(secondRetryDestinationTopic);
		assertThat(destinationMap.containsKey(secondRetryDestinationTopic.getDestinationName())).isTrue();
		assertThat(destinationMap.get(secondRetryDestinationTopic.getDestinationName()).getSourceDestination()).isEqualTo(secondRetryDestinationTopic);
		assertThat(destinationMap.get(secondRetryDestinationTopic.getDestinationName()).getNextDestination()).isEqualTo(dltDestinationTopic);
		assertThat(destinationMap.containsKey(dltDestinationTopic.getDestinationName())).isTrue();
		assertThat(destinationMap.get(dltDestinationTopic.getDestinationName()).getSourceDestination()).isEqualTo(dltDestinationTopic);
		assertThat(destinationMap.get(dltDestinationTopic.getDestinationName()).getNextDestination()).isEqualTo(noOpsDestinationTopic);

		assertThat(destinationMap.containsKey(mainDestinationTopic2.getDestinationName())).isTrue();
		assertThat(destinationMap.get(mainDestinationTopic2.getDestinationName()).getSourceDestination()).isEqualTo(mainDestinationTopic2);
		assertThat(destinationMap.get(mainDestinationTopic2.getDestinationName()).getNextDestination()).isEqualTo(firstRetryDestinationTopic2);
		assertThat(destinationMap.containsKey(firstRetryDestinationTopic2.getDestinationName())).isTrue();
		assertThat(destinationMap.get(firstRetryDestinationTopic2.getDestinationName()).getSourceDestination()).isEqualTo(firstRetryDestinationTopic2);
		assertThat(destinationMap.get(firstRetryDestinationTopic2.getDestinationName()).getNextDestination()).isEqualTo(secondRetryDestinationTopic2);
		assertThat(destinationMap.containsKey(secondRetryDestinationTopic2.getDestinationName())).isTrue();
		assertThat(destinationMap.get(secondRetryDestinationTopic2.getDestinationName()).getSourceDestination()).isEqualTo(secondRetryDestinationTopic2);
		assertThat(destinationMap.get(secondRetryDestinationTopic2.getDestinationName()).getNextDestination()).isEqualTo(dltDestinationTopic2);
		assertThat(destinationMap.containsKey(dltDestinationTopic2.getDestinationName())).isTrue();
		assertThat(destinationMap.get(dltDestinationTopic2.getDestinationName()).getSourceDestination()).isEqualTo(dltDestinationTopic2);
		assertThat(destinationMap.get(dltDestinationTopic2.getDestinationName()).getNextDestination()).isEqualTo(noOpsDestinationTopic2);

		assertThat(destinationMap.containsKey(mainDestinationTopic3.getDestinationName())).isTrue();
		assertThat(destinationMap.get(mainDestinationTopic3.getDestinationName()).getSourceDestination()).isEqualTo(mainDestinationTopic3);
		assertThat(destinationMap.get(mainDestinationTopic3.getDestinationName()).getNextDestination()).isEqualTo(firstRetryDestinationTopic3);
		assertThat(destinationMap.containsKey(firstRetryDestinationTopic3.getDestinationName())).isTrue();
		assertThat(destinationMap.get(firstRetryDestinationTopic3.getDestinationName()).getSourceDestination()).isEqualTo(firstRetryDestinationTopic3);
		assertThat(destinationMap.get(firstRetryDestinationTopic3.getDestinationName()).getNextDestination()).isEqualTo(secondRetryDestinationTopic3);
		assertThat(destinationMap.containsKey(secondRetryDestinationTopic3.getDestinationName())).isTrue();
		assertThat(destinationMap.get(secondRetryDestinationTopic3.getDestinationName()).getSourceDestination()).isEqualTo(secondRetryDestinationTopic3);
		assertThat(destinationMap.get(secondRetryDestinationTopic3.getDestinationName()).getNextDestination()).isEqualTo(noOpsDestinationTopic3);
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
		assertThat(allProcessedTopics).isEqualTo(allTopics);

	}
}
