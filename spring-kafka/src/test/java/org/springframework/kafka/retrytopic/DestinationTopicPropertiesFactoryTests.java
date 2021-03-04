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

	private final FixedDelayStrategy fixedDelayStrategy =
			FixedDelayStrategy.SINGLE_TOPIC;

	private final TopicSuffixingStrategy defaultTopicSuffixingStrategy =
			TopicSuffixingStrategy.SUFFIX_WITH_DELAY_VALUE;

	private final TopicSuffixingStrategy suffixWithIndexTopicSuffixingStrategy =
			TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE;

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
		List<DestinationTopic.Properties> propertiesList =
				new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix, 1,
						backOffPolicy, classifier, numPartitions, kafkaOperations, fixedDelayStrategy,
						dltStrategy, defaultTopicSuffixingStrategy, RetryTopicConstants.NOT_SET)
						.createProperties();

		// then
		assertThat(propertiesList.size() == 2).isTrue();
		DestinationTopic.Properties mainTopicProperties = propertiesList.get(0);
		assertThat(mainTopicProperties.suffix()).isEqualTo("");
		assertThat(mainTopicProperties.isDltTopic()).isFalse();
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
		DestinationTopic dltTopic = new DestinationTopic("mainTopic", dltProperties);
		assertThat(dltTopic.getDestinationDelay()).isEqualTo(0);
		assertThat(dltTopic.shouldRetryOn(0, new IllegalArgumentException())).isFalse();
		assertThat(dltTopic.shouldRetryOn(maxAttempts, new IllegalArgumentException())).isFalse();
		assertThat(dltTopic.shouldRetryOn(0, new RuntimeException())).isFalse();
		assertThat(dltTopic.getDestinationTimeout()).isEqualTo(RetryTopicConstants.NOT_SET);
	}

	@Test
	void shouldCreateTwoRetryPropertiesForMultipleBackoffValues() {
		// when
		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		backOffPolicy.setInitialInterval(1000);
		backOffPolicy.setMultiplier(2);
		int maxAttempts = 3;

		List<DestinationTopic.Properties> propertiesList =
				new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix, maxAttempts,
						backOffPolicy, classifier, numPartitions, kafkaOperations, fixedDelayStrategy,
						dltStrategy, TopicSuffixingStrategy.SUFFIX_WITH_DELAY_VALUE, RetryTopicConstants.NOT_SET)
						.createProperties();

		List<DestinationTopic> destinationTopicList = propertiesList
				.stream()
				.map(properties -> new DestinationTopic("mainTopic" + properties.suffix(), properties))
				.collect(Collectors.toList());

		// then
		assertThat(propertiesList.size() == 4).isTrue();
		DestinationTopic.Properties firstRetryProperties = propertiesList.get(1);
		assertThat(firstRetryProperties.suffix()).isEqualTo(retryTopicSuffix + "-1000");
		assertThat(firstRetryProperties.isDltTopic()).isFalse();
		DestinationTopic firstRetryDestinationTopic = destinationTopicList.get(1);
		assertThat(firstRetryDestinationTopic.getDestinationDelay()).isEqualTo(1000);
		assertThat(firstRetryDestinationTopic.getDestinationPartitions()).isEqualTo(numPartitions);
		assertThat(firstRetryDestinationTopic.shouldRetryOn(0, new IllegalArgumentException())).isTrue();
		assertThat(firstRetryDestinationTopic.shouldRetryOn(maxAttempts, new IllegalArgumentException())).isFalse();
		assertThat(firstRetryDestinationTopic.shouldRetryOn(0, new RuntimeException())).isFalse();

		DestinationTopic.Properties secondRetryProperties = propertiesList.get(2);
		assertThat(secondRetryProperties.suffix()).isEqualTo(retryTopicSuffix + "-2000");
		assertThat(secondRetryProperties.isDltTopic()).isFalse();
		DestinationTopic secondRetryDestinationTopic = destinationTopicList.get(2);
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

		List<DestinationTopic.Properties> propertiesList =
				new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix, maxAttempts,
						backOffPolicy, classifier, numPartitions, kafkaOperations, fixedDelayStrategy,
						noDltStrategy, TopicSuffixingStrategy.SUFFIX_WITH_DELAY_VALUE, RetryTopicConstants.NOT_SET)
						.createProperties();

		List<DestinationTopic> destinationTopicList = propertiesList
				.stream()
				.map(properties -> new DestinationTopic("mainTopic" + properties.suffix(), properties))
				.collect(Collectors.toList());

		// then
		assertThat(propertiesList.size() == 3).isTrue();
		assertThat(propertiesList.get(2).isDltTopic()).isFalse();
	}

	@Test
	void shouldCreateOneRetryPropertyForFixedBackoffWithSingleTopicStrategy() {

		// when
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		int maxAttempts = 5;

		List<DestinationTopic.Properties> propertiesList =
				new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix, maxAttempts,
						backOffPolicy, classifier, numPartitions, kafkaOperations, FixedDelayStrategy.SINGLE_TOPIC,
						dltStrategy, defaultTopicSuffixingStrategy, -1).createProperties();

		List<DestinationTopic> destinationTopicList = propertiesList
				.stream()
				.map(properties -> new DestinationTopic("mainTopic" + properties.suffix(), properties))
				.collect(Collectors.toList());

		// then
		assertThat(propertiesList.size() == 3).isTrue();

		DestinationTopic.Properties mainTopicProperties = propertiesList.get(0);
		DestinationTopic mainDestinationTopic = destinationTopicList.get(0);
		assertThat(mainDestinationTopic.isMainTopic()).isTrue();

		DestinationTopic.Properties firstRetryProperties = propertiesList.get(1);
		assertThat(firstRetryProperties.suffix()).isEqualTo(retryTopicSuffix);
		DestinationTopic retryDestinationTopic = destinationTopicList.get(1);
		assertThat(retryDestinationTopic.isSingleTopicRetry()).isTrue();
		assertThat(retryDestinationTopic.getDestinationDelay()).isEqualTo(1000);

		DestinationTopic.Properties dltProperties = propertiesList.get(2);
		assertThat(dltProperties.suffix()).isEqualTo(dltSuffix);
		assertThat(dltProperties.isDltTopic()).isTrue();
		DestinationTopic dltTopic = destinationTopicList.get(2);
		assertThat(dltTopic.getDestinationDelay()).isEqualTo(0);
		assertThat(dltTopic.getDestinationPartitions()).isEqualTo(numPartitions);
	}

	@Test
	void shouldCreateRetryPropertiesForFixedBackoffWithMultiTopicStrategy() {

		// when
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(5000);
		int maxAttempts = 3;

		List<DestinationTopic.Properties> propertiesList =
				new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix, maxAttempts,
						backOffPolicy, classifier, numPartitions, kafkaOperations,
						FixedDelayStrategy.MULTIPLE_TOPICS,
						dltStrategy, defaultTopicSuffixingStrategy, -1).createProperties();

		List<DestinationTopic> destinationTopicList = propertiesList
				.stream()
				.map(properties -> new DestinationTopic("mainTopic" + properties.suffix(), properties))
				.collect(Collectors.toList());

		// then
		assertThat(propertiesList.size() == 4).isTrue();

		DestinationTopic.Properties mainTopicProperties = propertiesList.get(0);
		DestinationTopic mainDestinationTopic = destinationTopicList.get(0);
		assertThat(mainDestinationTopic.isMainTopic()).isTrue();

		DestinationTopic.Properties firstRetryProperties = propertiesList.get(1);
		assertThat(firstRetryProperties.suffix()).isEqualTo(retryTopicSuffix + "-0");
		DestinationTopic retryDestinationTopic = destinationTopicList.get(1);
		assertThat(retryDestinationTopic.isSingleTopicRetry()).isFalse();
		assertThat(retryDestinationTopic.getDestinationDelay()).isEqualTo(5000);

		DestinationTopic.Properties secondRetryProperties = propertiesList.get(2);
		assertThat(secondRetryProperties.suffix()).isEqualTo(retryTopicSuffix + "-1");
		DestinationTopic secondRetryDestinationTopic = destinationTopicList.get(2);
		assertThat(secondRetryDestinationTopic.isSingleTopicRetry()).isFalse();
		assertThat(secondRetryDestinationTopic.getDestinationDelay()).isEqualTo(5000);

		DestinationTopic.Properties dltProperties = propertiesList.get(3);
		assertThat(dltProperties.suffix()).isEqualTo(dltSuffix);
		assertThat(dltProperties.isDltTopic()).isTrue();
		DestinationTopic dltTopic = destinationTopicList.get(3);
		assertThat(dltTopic.getDestinationDelay()).isEqualTo(0);
		assertThat(dltTopic.getDestinationPartitions()).isEqualTo(numPartitions);
	}

	@Test
	void shouldSuffixRetryTopicsWithIndexIfSuffixWithIndexStrategy() {

		// setup
		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		int maxAttempts = 3;

		// when
		List<DestinationTopic.Properties> propertiesList =
				new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix, maxAttempts,
						backOffPolicy, classifier, numPartitions, kafkaOperations,
						FixedDelayStrategy.SINGLE_TOPIC,
						dltStrategy, suffixWithIndexTopicSuffixingStrategy, -1).createProperties();

		// then
		IntStream.range(1, maxAttempts)
				.forEach(index -> assertThat(propertiesList.get(index).suffix()).isEqualTo(retryTopicSuffix + "-" + String.valueOf(index - 1)));
	}

	@Test
	void shouldSuffixRetryTopicsWithIndexIfFixedDelayWithMultipleTopics() {

		// setup
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		int maxAttempts = 3;

		// when
		List<DestinationTopic.Properties> propertiesList =
				new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix, maxAttempts,
						backOffPolicy, classifier, numPartitions, kafkaOperations,
						FixedDelayStrategy.MULTIPLE_TOPICS,
						dltStrategy, suffixWithIndexTopicSuffixingStrategy, -1).createProperties();

		// then
		IntStream.range(1, maxAttempts)
				.forEach(index -> assertThat(propertiesList.get(index).suffix()).isEqualTo(retryTopicSuffix + "-" + String.valueOf(index - 1)));
	}

	@Test
	void shouldSuffixRetryTopicsWithMixedIfMaxDelayReached() {

		// setup
		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		backOffPolicy.setInitialInterval(1000);
		backOffPolicy.setMultiplier(2);
		backOffPolicy.setMaxInterval(3000);
		int maxAttempts = 5;

		// when
		List<DestinationTopic.Properties> propertiesList =
				new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix, maxAttempts,
						backOffPolicy, classifier, numPartitions, kafkaOperations,
						FixedDelayStrategy.MULTIPLE_TOPICS,
						dltStrategy, defaultTopicSuffixingStrategy, -1).createProperties();

		// then
		assertThat(propertiesList.size() == 6).isTrue();
		assertThat(propertiesList.get(0).suffix()).isEqualTo("");
		assertThat(propertiesList.get(1).suffix()).isEqualTo(retryTopicSuffix + "-1000");
		assertThat(propertiesList.get(2).suffix()).isEqualTo(retryTopicSuffix + "-2000");
		assertThat(propertiesList.get(3).suffix()).isEqualTo(retryTopicSuffix + "-3000-0");
		assertThat(propertiesList.get(4).suffix()).isEqualTo(retryTopicSuffix + "-3000-1");
		assertThat(propertiesList.get(5).suffix()).isEqualTo(dltSuffix);
	}
}
