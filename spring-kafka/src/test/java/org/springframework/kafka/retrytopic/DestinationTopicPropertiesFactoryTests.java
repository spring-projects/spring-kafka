/*
 * Copyright 2018-2023 the original author or authors.
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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.classify.BinaryExceptionClassifierBuilder;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;


/**
 * @author Tomaz Fernandes
 * @since 2.7
 */
@ExtendWith(MockitoExtension.class)
class DestinationTopicPropertiesFactoryTests {

	private final String retryTopicSuffix = "test-retry-suffix";

	private final String dltSuffix = "test-dlt-suffix";

	private final int maxAttempts = 4;

	private final int numPartitions = 0;

	@SuppressWarnings("deprecation")
	private final FixedDelayStrategy fixedDelayStrategy =
			FixedDelayStrategy.SINGLE_TOPIC;

	private final TopicSuffixingStrategy suffixWithDelayValueSuffixingStrategy =
			TopicSuffixingStrategy.SUFFIX_WITH_DELAY_VALUE;

	private final TopicSuffixingStrategy suffixWithIndexTopicSuffixingStrategy =
			TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE;

	private final SameIntervalTopicReuseStrategy multipleTopicsSameIntervalReuseStrategy =
			SameIntervalTopicReuseStrategy.MULTIPLE_TOPICS;

	private final SameIntervalTopicReuseStrategy singleTopicSameIntervalReuseStrategy =
			SameIntervalTopicReuseStrategy.SINGLE_TOPIC;

	private final DltStrategy dltStrategy =
			DltStrategy.FAIL_ON_ERROR;

	private final DltStrategy noDltStrategy =
			DltStrategy.NO_DLT;

	private final BackOffPolicy backOffPolicy = new FixedBackOffPolicy();

	private final BinaryExceptionClassifier classifier = new BinaryExceptionClassifierBuilder()
			.retryOn(IllegalArgumentException.class).build();

	@Mock
	private KafkaOperations<?, ?> kafkaOperations;

	@BeforeEach
	void setup() {
		((FixedBackOffPolicy) backOffPolicy).setBackOffPeriod(1000);
	}

	@Test
	void shouldCreateMainAndDltProperties() {
		// when

		List<Long> backOffValues = new BackOffValuesGenerator(1, backOffPolicy).generateValues();

		List<DestinationTopic.Properties> propertiesList =
				new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix, backOffValues,
						classifier, numPartitions, kafkaOperations, fixedDelayStrategy,
						dltStrategy, suffixWithDelayValueSuffixingStrategy, multipleTopicsSameIntervalReuseStrategy,
						RetryTopicConstants.NOT_SET).createProperties();

		// then
		assertThat(propertiesList.size() == 2).isTrue();
		DestinationTopic.Properties mainTopicProperties = propertiesList.get(0);
		assertThat(mainTopicProperties.suffix()).isEqualTo("");
		assertThat(mainTopicProperties.isDltTopic()).isFalse();
		assertThat(mainTopicProperties.isRetryTopic()).isFalse();
		DestinationTopic mainTopic = new DestinationTopic("mainTopic", mainTopicProperties);
		assertThat(mainTopic.getDestinationDelay()).isEqualTo(0L);
		assertThat(mainTopic.shouldRetryOn(0, new IllegalArgumentException())).isTrue();
		assertThat(mainTopic.shouldRetryOn(maxAttempts, new IllegalArgumentException())).isFalse();
		assertThat(mainTopic.shouldRetryOn(0, new RuntimeException())).isFalse();
		assertThat(mainTopic.getDestinationTimeout()).isEqualTo(RetryTopicConstants.NOT_SET);

		DestinationTopic.Properties dltProperties = propertiesList.get(1);
		assertDltTopic(dltProperties);
	}

	private void assertDltTopic(DestinationTopic.Properties dltProperties) {
		assertThat(dltProperties.suffix()).isEqualTo(dltSuffix);
		assertThat(dltProperties.isDltTopic()).isTrue();
		assertThat(dltProperties.isRetryTopic()).isFalse();
		DestinationTopic dltTopic = new DestinationTopic("mainTopic", dltProperties);
		assertThat(dltTopic.getDestinationDelay()).isEqualTo(0);
		assertThat(dltTopic.shouldRetryOn(0, new IllegalArgumentException())).isFalse();
		assertThat(dltTopic.shouldRetryOn(maxAttempts, new IllegalArgumentException())).isFalse();
		assertThat(dltTopic.shouldRetryOn(0, new RuntimeException())).isFalse();
		assertThat(dltTopic.getDestinationTimeout()).isEqualTo(RetryTopicConstants.NOT_SET);
	}

	@Test
	@SuppressWarnings("deprecation")
	void shouldCreateTwoRetryPropertiesForMultipleBackoffValues() {
		// when
		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		backOffPolicy.setInitialInterval(1000);
		backOffPolicy.setMultiplier(2);
		int maxAttempts = 3;

		List<Long> backOffValues = new BackOffValuesGenerator(maxAttempts, backOffPolicy).generateValues();

		List<DestinationTopic.Properties> propertiesList =
				new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix, backOffValues,
						classifier, numPartitions, kafkaOperations, fixedDelayStrategy,
						dltStrategy, TopicSuffixingStrategy.SUFFIX_WITH_DELAY_VALUE,
						multipleTopicsSameIntervalReuseStrategy, RetryTopicConstants.NOT_SET).createProperties();

		List<DestinationTopic> destinationTopicList = propertiesList
				.stream()
				.map(properties -> new DestinationTopic("mainTopic" + properties.suffix(), properties))
				.collect(Collectors.toList());

		// then
		assertThat(propertiesList.size() == 4).isTrue();
		DestinationTopic.Properties firstRetryProperties = propertiesList.get(1);
		assertThat(firstRetryProperties.suffix()).isEqualTo(retryTopicSuffix + "-1000");
		assertThat(firstRetryProperties.isDltTopic()).isFalse();
		assertThat(firstRetryProperties.isRetryTopic()).isTrue();
		DestinationTopic firstRetryDestinationTopic = destinationTopicList.get(1);
		assertThat(firstRetryDestinationTopic.isSingleTopicRetry()).isFalse();
		assertThat(firstRetryDestinationTopic.isReusableRetryTopic()).isFalse();
		assertThat(firstRetryDestinationTopic.getDestinationDelay()).isEqualTo(1000);
		assertThat(firstRetryDestinationTopic.getDestinationPartitions()).isEqualTo(numPartitions);
		assertThat(firstRetryDestinationTopic.shouldRetryOn(0, new IllegalArgumentException())).isTrue();
		assertThat(firstRetryDestinationTopic.shouldRetryOn(maxAttempts, new IllegalArgumentException())).isFalse();
		assertThat(firstRetryDestinationTopic.shouldRetryOn(0, new RuntimeException())).isFalse();

		DestinationTopic.Properties secondRetryProperties = propertiesList.get(2);
		assertThat(secondRetryProperties.suffix()).isEqualTo(retryTopicSuffix + "-2000");
		assertThat(secondRetryProperties.isDltTopic()).isFalse();
		assertThat(secondRetryProperties.isRetryTopic()).isTrue();
		DestinationTopic secondRetryDestinationTopic = destinationTopicList.get(2);
		assertThat(secondRetryDestinationTopic.isSingleTopicRetry()).isFalse();
		assertThat(secondRetryDestinationTopic.isReusableRetryTopic()).isFalse();
		assertThat(secondRetryDestinationTopic.getDestinationDelay()).isEqualTo(2000);
		assertThat(secondRetryDestinationTopic.getDestinationPartitions()).isEqualTo(numPartitions);
		assertThat(secondRetryDestinationTopic.shouldRetryOn(0, new IllegalArgumentException())).isTrue();
		assertThat(secondRetryDestinationTopic.shouldRetryOn(maxAttempts, new IllegalArgumentException())).isFalse();
		assertThat(secondRetryDestinationTopic.shouldRetryOn(0, new RuntimeException())).isFalse();

		assertDltTopic(propertiesList.get(3));
	}

	@Test
	void shouldNotCreateDltProperties() {

		// when
		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		backOffPolicy.setInitialInterval(1000);
		backOffPolicy.setMultiplier(2);
		int maxAttempts = 3;

		List<Long> backOffValues = new BackOffValuesGenerator(maxAttempts, backOffPolicy).generateValues();

		List<DestinationTopic.Properties> propertiesList =
				new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix, backOffValues, classifier,
						numPartitions, kafkaOperations, fixedDelayStrategy, noDltStrategy,
						TopicSuffixingStrategy.SUFFIX_WITH_DELAY_VALUE, multipleTopicsSameIntervalReuseStrategy,
						RetryTopicConstants.NOT_SET).createProperties();

		// then
		assertThat(propertiesList.size() == 3).isTrue();
		assertThat(propertiesList.get(2).isDltTopic()).isFalse();
	}

	@Test
	@SuppressWarnings("deprecation")
	void shouldCreateOneRetryPropertyForFixedBackoffWithSingleTopicStrategy() {

		// when
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		int maxAttempts = 5;

		List<Long> backOffValues = new BackOffValuesGenerator(maxAttempts, backOffPolicy).generateValues();

		List<DestinationTopic.Properties> propertiesList =
				new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix, backOffValues,
						classifier, numPartitions, kafkaOperations, FixedDelayStrategy.SINGLE_TOPIC,
						dltStrategy, suffixWithDelayValueSuffixingStrategy, multipleTopicsSameIntervalReuseStrategy,
						-1).createProperties();

		List<DestinationTopic> destinationTopicList = propertiesList
				.stream()
				.map(properties -> new DestinationTopic("mainTopic" + properties.suffix(), properties))
				.collect(Collectors.toList());

		// then
		assertThat(propertiesList.size() == 3).isTrue();

		DestinationTopic mainDestinationTopic = destinationTopicList.get(0);
		assertThat(mainDestinationTopic.isMainTopic()).isTrue();

		DestinationTopic.Properties firstRetryProperties = propertiesList.get(1);
		assertThat(firstRetryProperties.suffix()).isEqualTo(retryTopicSuffix);
		assertThat(firstRetryProperties.isRetryTopic()).isTrue();
		DestinationTopic retryDestinationTopic = destinationTopicList.get(1);
		assertThat(retryDestinationTopic.isSingleTopicRetry()).isTrue();
		assertThat(retryDestinationTopic.isReusableRetryTopic()).isFalse();
		assertThat(retryDestinationTopic.getDestinationDelay()).isEqualTo(1000);

		DestinationTopic.Properties dltProperties = propertiesList.get(2);
		assertThat(dltProperties.suffix()).isEqualTo(dltSuffix);
		assertThat(dltProperties.isDltTopic()).isTrue();
		DestinationTopic dltTopic = destinationTopicList.get(2);
		assertThat(dltTopic.getDestinationDelay()).isEqualTo(0);
		assertThat(dltTopic.getDestinationPartitions()).isEqualTo(numPartitions);
	}

	@Test
	@SuppressWarnings("deprecation")
	void shouldCreateOneRetryPropertyForFixedBackoffWithSingleTopicSameIntervalReuseStrategy() {

		// when
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		int maxAttempts = 5;

		List<Long> backOffValues = new BackOffValuesGenerator(maxAttempts, backOffPolicy).generateValues();

		List<DestinationTopic.Properties> propertiesList =
				new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix, backOffValues,
						classifier, numPartitions, kafkaOperations, FixedDelayStrategy.MULTIPLE_TOPICS,
						dltStrategy, suffixWithDelayValueSuffixingStrategy, singleTopicSameIntervalReuseStrategy,
						-1).createProperties();

		List<DestinationTopic> destinationTopicList = propertiesList
				.stream()
				.map(properties -> new DestinationTopic("mainTopic" + properties.suffix(), properties))
				.collect(Collectors.toList());

		// then
		assertThat(propertiesList.size() == 3).isTrue();

		DestinationTopic mainDestinationTopic = destinationTopicList.get(0);
		assertThat(mainDestinationTopic.isMainTopic()).isTrue();

		DestinationTopic.Properties firstRetryProperties = propertiesList.get(1);
		assertThat(firstRetryProperties.suffix()).isEqualTo(retryTopicSuffix);
		DestinationTopic retryDestinationTopic = destinationTopicList.get(1);
		assertThat(retryDestinationTopic.isSingleTopicRetry()).isTrue();
		assertThat(retryDestinationTopic.isReusableRetryTopic()).isFalse();
		assertThat(retryDestinationTopic.getDestinationDelay()).isEqualTo(1000);

		DestinationTopic.Properties dltProperties = propertiesList.get(2);
		assertThat(dltProperties.suffix()).isEqualTo(dltSuffix);
		assertThat(dltProperties.isDltTopic()).isTrue();
		DestinationTopic dltTopic = destinationTopicList.get(2);
		assertThat(dltTopic.getDestinationDelay()).isEqualTo(0);
		assertThat(dltTopic.getDestinationPartitions()).isEqualTo(numPartitions);
	}

	@Test
	@SuppressWarnings("deprecation")
	void shouldCreateRetryPropertiesForFixedBackoffWithMultiTopicStrategy() {

		// when
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(5000);
		int maxAttempts = 3;

		List<Long> backOffValues = new BackOffValuesGenerator(maxAttempts, backOffPolicy).generateValues();

		List<DestinationTopic.Properties> propertiesList =
				new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix, backOffValues,
						classifier, numPartitions, kafkaOperations,
						FixedDelayStrategy.MULTIPLE_TOPICS,
						dltStrategy, suffixWithDelayValueSuffixingStrategy, multipleTopicsSameIntervalReuseStrategy,
						-1).createProperties();

		List<DestinationTopic> destinationTopicList = propertiesList
				.stream()
				.map(properties -> new DestinationTopic("mainTopic" + properties.suffix(), properties))
				.collect(Collectors.toList());

		// then
		assertThat(propertiesList.size() == 4).isTrue();

		DestinationTopic mainDestinationTopic = destinationTopicList.get(0);
		assertThat(mainDestinationTopic.isMainTopic()).isTrue();

		DestinationTopic.Properties firstRetryProperties = propertiesList.get(1);
		assertThat(firstRetryProperties.suffix()).isEqualTo(retryTopicSuffix + "-0");
		assertThat(firstRetryProperties.isRetryTopic()).isTrue();
		DestinationTopic retryDestinationTopic = destinationTopicList.get(1);
		assertThat(retryDestinationTopic.isSingleTopicRetry()).isFalse();
		assertThat(retryDestinationTopic.isReusableRetryTopic()).isFalse();
		assertThat(retryDestinationTopic.getDestinationDelay()).isEqualTo(5000);

		DestinationTopic.Properties secondRetryProperties = propertiesList.get(2);
		assertThat(secondRetryProperties.suffix()).isEqualTo(retryTopicSuffix + "-1");
		DestinationTopic secondRetryDestinationTopic = destinationTopicList.get(2);
		assertThat(secondRetryDestinationTopic.isSingleTopicRetry()).isFalse();
		assertThat(secondRetryDestinationTopic.isReusableRetryTopic()).isFalse();
		assertThat(secondRetryDestinationTopic.getDestinationDelay()).isEqualTo(5000);

		DestinationTopic.Properties dltProperties = propertiesList.get(3);
		assertThat(dltProperties.suffix()).isEqualTo(dltSuffix);
		assertThat(dltProperties.isDltTopic()).isTrue();
		DestinationTopic dltTopic = destinationTopicList.get(3);
		assertThat(dltTopic.getDestinationDelay()).isEqualTo(0);
		assertThat(dltTopic.getDestinationPartitions()).isEqualTo(numPartitions);
	}

	@Test
	@SuppressWarnings("deprecation")
	void shouldSuffixRetryTopicsWithIndexIfSuffixWithIndexStrategy() {

		// setup
		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		int maxAttempts = 3;
		List<Long> backOffValues = new BackOffValuesGenerator(maxAttempts, backOffPolicy).generateValues();

		// when
		List<DestinationTopic.Properties> propertiesList =
				new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix, backOffValues,
						classifier, numPartitions, kafkaOperations,
						FixedDelayStrategy.SINGLE_TOPIC,
						dltStrategy, suffixWithIndexTopicSuffixingStrategy,
						multipleTopicsSameIntervalReuseStrategy, -1).createProperties();

		// then
		IntStream.range(1, maxAttempts).forEach(index -> assertThat(propertiesList.get(index).suffix())
				.isEqualTo(retryTopicSuffix + "-" + String.valueOf(index - 1)));
	}

	@Test
	@SuppressWarnings("deprecation")
	void shouldSuffixRetryTopicsWithIndexIfFixedDelayWithMultipleTopics() {

		// setup
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		int maxAttempts = 3;
		List<Long> backOffValues = new BackOffValuesGenerator(maxAttempts, backOffPolicy).generateValues();

		// when
		List<DestinationTopic.Properties> propertiesList =
				new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix, backOffValues,
						classifier, numPartitions, kafkaOperations,
						FixedDelayStrategy.MULTIPLE_TOPICS,
						dltStrategy, suffixWithIndexTopicSuffixingStrategy, multipleTopicsSameIntervalReuseStrategy,
						-1).createProperties();

		// then
		IntStream.range(1, maxAttempts)
				.forEach(index -> assertThat(propertiesList.get(index).suffix()).isEqualTo(retryTopicSuffix +
						"-" + String.valueOf(index - 1)));
	}

	@Test
	@SuppressWarnings("deprecation")
	void shouldSuffixRetryTopicsWithMixedIfMaxDelayReached() {

		// setup
		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		backOffPolicy.setInitialInterval(1000);
		backOffPolicy.setMultiplier(2);
		backOffPolicy.setMaxInterval(3000);
		int maxAttempts = 5;
		List<Long> backOffValues = new BackOffValuesGenerator(maxAttempts, backOffPolicy).generateValues();

		// when
		DestinationTopicPropertiesFactory factory = new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix,
				backOffValues, classifier, numPartitions, kafkaOperations,
				FixedDelayStrategy.MULTIPLE_TOPICS,
				dltStrategy, suffixWithDelayValueSuffixingStrategy, multipleTopicsSameIntervalReuseStrategy, -1);

		List<DestinationTopic.Properties> propertiesList = factory.createProperties();

		// then
		assertThat(factory.retryTopicsAmount() == 4).isTrue();
		assertThat(propertiesList.size() == 6).isTrue();
		assertThat(propertiesList.get(0).suffix()).isEqualTo("");
		assertThat(propertiesList.get(1).suffix()).isEqualTo(retryTopicSuffix + "-1000");
		assertThat(propertiesList.get(2).suffix()).isEqualTo(retryTopicSuffix + "-2000");
		assertThat(propertiesList.get(3).suffix()).isEqualTo(retryTopicSuffix + "-3000-0");
		assertThat(propertiesList.get(4).suffix()).isEqualTo(retryTopicSuffix + "-3000-1");
		assertThat(propertiesList.get(5).suffix()).isEqualTo(dltSuffix);
	}

	@Test
	@SuppressWarnings("deprecation")
	void shouldReuseRetryTopicsIfMaxDelayReachedWithDelayValueSuffixingStrategy() {

		// setup
		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		backOffPolicy.setInitialInterval(1000);
		backOffPolicy.setMultiplier(2);
		backOffPolicy.setMaxInterval(3000);
		int maxAttempts = 5;
		List<Long> backOffValues = new BackOffValuesGenerator(maxAttempts, backOffPolicy).generateValues();

		// when
		DestinationTopicPropertiesFactory factory = new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix,
				backOffValues, classifier, numPartitions, kafkaOperations,
				FixedDelayStrategy.MULTIPLE_TOPICS,
				dltStrategy, suffixWithDelayValueSuffixingStrategy, singleTopicSameIntervalReuseStrategy, -1);

		List<DestinationTopic.Properties> propertiesList = factory.createProperties();

		// then
		assertThat(factory.retryTopicsAmount()).isEqualTo(3);
		assertThat(propertiesList.size()).isEqualTo(5);
		assertThat(propertiesList.get(0).suffix()).isEqualTo("");
		assertRetryTopic(propertiesList.get(1), maxAttempts, 1000L, retryTopicSuffix + "-1000", false, false);
		assertRetryTopic(propertiesList.get(2), maxAttempts, 2000L, retryTopicSuffix + "-2000", false, false);
		assertRetryTopic(propertiesList.get(3), maxAttempts, 3000L, retryTopicSuffix + "-3000", true, false);
		assertThat(propertiesList.get(4).suffix()).isEqualTo(dltSuffix);
	}

	@Test
	@SuppressWarnings("deprecation")
	void shouldReuseRetryTopicsIfMaxDelayReachedWithIndexValueSuffixingStrategy() {

		// setup
		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		backOffPolicy.setInitialInterval(1000);
		backOffPolicy.setMultiplier(2);
		backOffPolicy.setMaxInterval(3000);
		int maxAttempts = 5;
		List<Long> backOffValues = new BackOffValuesGenerator(maxAttempts, backOffPolicy).generateValues();

		// when
		DestinationTopicPropertiesFactory factory = new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix,
				backOffValues, classifier, numPartitions, kafkaOperations,
				FixedDelayStrategy.MULTIPLE_TOPICS,
				dltStrategy, suffixWithIndexTopicSuffixingStrategy, singleTopicSameIntervalReuseStrategy, -1);

		List<DestinationTopic.Properties> propertiesList = factory.createProperties();

		// then
		assertThat(factory.retryTopicsAmount()).isEqualTo(3);
		assertThat(propertiesList.size()).isEqualTo(5);
		assertThat(propertiesList.get(0).suffix()).isEqualTo("");
		assertRetryTopic(propertiesList.get(1), maxAttempts, 1000L, retryTopicSuffix + "-0", false, false);
		assertRetryTopic(propertiesList.get(2), maxAttempts, 2000L, retryTopicSuffix + "-1", false, false);
		assertRetryTopic(propertiesList.get(3), maxAttempts, 3000L, retryTopicSuffix + "-2", true, false);
		assertThat(propertiesList.get(4).suffix()).isEqualTo(dltSuffix);
	}

	@Test
	@SuppressWarnings("deprecation")
	void shouldNotReuseRetryTopicsIfRepeatedIntervalsAreInTheMiddleOfChain() {

		// setup
		List<Long> backOffValues = List.of(1000L, 2000L, 2000L, 2000L, 3000L);
		int maxAttempts = backOffValues.size() + 1;

		// when
		DestinationTopicPropertiesFactory factory = new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix,
				backOffValues, classifier, numPartitions, kafkaOperations,
				FixedDelayStrategy.SINGLE_TOPIC,
				dltStrategy, suffixWithDelayValueSuffixingStrategy, multipleTopicsSameIntervalReuseStrategy, -1);

		List<DestinationTopic.Properties> propertiesList = factory.createProperties();

		// then
		assertThat(factory.retryTopicsAmount()).isEqualTo(5);
		assertThat(propertiesList.size()).isEqualTo(7);
		assertThat(propertiesList.get(0).suffix()).isEqualTo("");
		assertRetryTopic(propertiesList.get(1), maxAttempts, 1000L, retryTopicSuffix + "-1000", false, false);
		assertRetryTopic(propertiesList.get(2), maxAttempts, 2000L, retryTopicSuffix + "-2000-0", false, false);
		assertRetryTopic(propertiesList.get(3), maxAttempts, 2000L, retryTopicSuffix + "-2000-1", false, false);
		assertRetryTopic(propertiesList.get(4), maxAttempts, 2000L, retryTopicSuffix + "-2000-2", false, false);
		assertRetryTopic(propertiesList.get(5), maxAttempts, 3000L, retryTopicSuffix + "-3000", false, false);
		assertThat(propertiesList.get(6).suffix()).isEqualTo(dltSuffix);
	}

	@SuppressWarnings("deprecation")
	private void assertRetryTopic(DestinationTopic.Properties topicProperties, int maxAttempts,
			Long expectedDelay, String expectedSuffix, boolean expectedReusableTopic,
			boolean expectedIsSingleTopicRetry) {
		assertThat(topicProperties.suffix()).isEqualTo(expectedSuffix);
		assertThat(topicProperties.isRetryTopic()).isTrue();
		DestinationTopic topic = new DestinationTopic("irrelevant" + topicProperties.suffix(), topicProperties);
		assertThat(topic.isDltTopic()).isFalse();
		assertThat(topic.isSingleTopicRetry()).isEqualTo(expectedIsSingleTopicRetry);
		assertThat(topic.isReusableRetryTopic()).isEqualTo(expectedReusableTopic);
		assertThat(topic.getDestinationDelay()).isEqualTo(expectedDelay);
		assertThat(topic.getDestinationPartitions()).isEqualTo(numPartitions);
		assertThat(topic.shouldRetryOn(0, new IllegalArgumentException())).isTrue();
		assertThat(topic.shouldRetryOn(maxAttempts, new IllegalArgumentException())).isFalse();
		assertThat(topic.shouldRetryOn(0, new RuntimeException())).isFalse();
	}
}
