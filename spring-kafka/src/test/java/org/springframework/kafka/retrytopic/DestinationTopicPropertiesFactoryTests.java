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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
		assertTrue(propertiesList.size() == 2);
		DestinationTopic.Properties mainTopicProperties = propertiesList.get(0);
		assertEquals("", mainTopicProperties.suffix());
		assertFalse(mainTopicProperties.isDltTopic());
		DestinationTopic mainTopic = new DestinationTopic("mainTopic", mainTopicProperties);
		assertEquals(0L, mainTopic.getDestinationDelay());
		assertTrue(mainTopic.shouldRetryOn(0, new IllegalArgumentException()));
		assertFalse(mainTopic.shouldRetryOn(maxAttempts, new IllegalArgumentException()));
		assertFalse(mainTopic.shouldRetryOn(0, new RuntimeException()));
		assertEquals(RetryTopicConstants.NOT_SET, mainTopic.getDestinationTimeout());

		DestinationTopic.Properties dltProperties = propertiesList.get(1);
		assertDltTopic(dltProperties);
	}

	private void assertDltTopic(DestinationTopic.Properties dltProperties) {
		assertEquals(dltSuffix, dltProperties.suffix());
		assertTrue(dltProperties.isDltTopic());
		DestinationTopic dltTopic = new DestinationTopic("mainTopic", dltProperties);
		assertEquals(0, dltTopic.getDestinationDelay());
		assertFalse(dltTopic.shouldRetryOn(0, new IllegalArgumentException()));
		assertFalse(dltTopic.shouldRetryOn(maxAttempts, new IllegalArgumentException()));
		assertFalse(dltTopic.shouldRetryOn(0, new RuntimeException()));
		assertEquals(RetryTopicConstants.NOT_SET, dltTopic.getDestinationTimeout());
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
		assertTrue(propertiesList.size() == 4);
		DestinationTopic.Properties firstRetryProperties = propertiesList.get(1);
		assertEquals(retryTopicSuffix + "-1000", firstRetryProperties.suffix());
		assertFalse(firstRetryProperties.isDltTopic());
		DestinationTopic firstRetryDestinationTopic = destinationTopicList.get(1);
		assertEquals(1000, firstRetryDestinationTopic.getDestinationDelay());
		assertEquals(numPartitions, firstRetryDestinationTopic.getDestinationPartitions());
		assertTrue(firstRetryDestinationTopic.shouldRetryOn(0, new IllegalArgumentException()));
		assertFalse(firstRetryDestinationTopic.shouldRetryOn(maxAttempts, new IllegalArgumentException()));
		assertFalse(firstRetryDestinationTopic.shouldRetryOn(0, new RuntimeException()));

		DestinationTopic.Properties secondRetryProperties = propertiesList.get(2);
		assertEquals(retryTopicSuffix + "-2000", secondRetryProperties.suffix());
		assertFalse(secondRetryProperties.isDltTopic());
		DestinationTopic secondRetryDestinationTopic = destinationTopicList.get(2);
		assertEquals(2000, secondRetryDestinationTopic.getDestinationDelay());
		assertEquals(numPartitions, secondRetryDestinationTopic.getDestinationPartitions());
		assertTrue(secondRetryDestinationTopic.shouldRetryOn(0, new IllegalArgumentException()));
		assertFalse(secondRetryDestinationTopic.shouldRetryOn(maxAttempts, new IllegalArgumentException()));
		assertFalse(secondRetryDestinationTopic.shouldRetryOn(0, new RuntimeException()));

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
		assertTrue(propertiesList.size() == 3);
		assertFalse(propertiesList.get(2).isDltTopic());
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
		assertTrue(propertiesList.size() == 3);

		DestinationTopic.Properties mainTopicProperties = propertiesList.get(0);
		DestinationTopic mainDestinationTopic = destinationTopicList.get(0);
		assertTrue(mainDestinationTopic.isMainTopic());

		DestinationTopic.Properties firstRetryProperties = propertiesList.get(1);
		assertEquals(retryTopicSuffix, firstRetryProperties.suffix());
		DestinationTopic retryDestinationTopic = destinationTopicList.get(1);
		assertTrue(retryDestinationTopic.isSingleTopicRetry());
		assertEquals(1000, retryDestinationTopic.getDestinationDelay());

		DestinationTopic.Properties dltProperties = propertiesList.get(2);
		assertEquals(dltSuffix, dltProperties.suffix());
		assertTrue(dltProperties.isDltTopic());
		DestinationTopic dltTopic = destinationTopicList.get(2);
		assertEquals(0, dltTopic.getDestinationDelay());
		assertEquals(numPartitions, dltTopic.getDestinationPartitions());
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
		assertTrue(propertiesList.size() == 4);

		DestinationTopic.Properties mainTopicProperties = propertiesList.get(0);
		DestinationTopic mainDestinationTopic = destinationTopicList.get(0);
		assertTrue(mainDestinationTopic.isMainTopic());

		DestinationTopic.Properties firstRetryProperties = propertiesList.get(1);
		assertEquals(retryTopicSuffix + "-0", firstRetryProperties.suffix());
		DestinationTopic retryDestinationTopic = destinationTopicList.get(1);
		assertFalse(retryDestinationTopic.isSingleTopicRetry());
		assertEquals(5000, retryDestinationTopic.getDestinationDelay());

		DestinationTopic.Properties secondRetryProperties = propertiesList.get(2);
		assertEquals(retryTopicSuffix + "-1", secondRetryProperties.suffix());
		DestinationTopic secondRetryDestinationTopic = destinationTopicList.get(2);
		assertFalse(secondRetryDestinationTopic.isSingleTopicRetry());
		assertEquals(5000, secondRetryDestinationTopic.getDestinationDelay());

		DestinationTopic.Properties dltProperties = propertiesList.get(3);
		assertEquals(dltSuffix, dltProperties.suffix());
		assertTrue(dltProperties.isDltTopic());
		DestinationTopic dltTopic = destinationTopicList.get(3);
		assertEquals(0, dltTopic.getDestinationDelay());
		assertEquals(numPartitions, dltTopic.getDestinationPartitions());
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
				.forEach(index -> assertEquals(retryTopicSuffix + "-" + String.valueOf(index - 1),
						propertiesList.get(index).suffix()));
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
				.forEach(index -> assertEquals(retryTopicSuffix + "-" + String.valueOf(index - 1),
						propertiesList.get(index).suffix()));
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
		assertTrue(propertiesList.size() == 6);
		assertEquals("", propertiesList.get(0).suffix());
		assertEquals(retryTopicSuffix + "-1000", propertiesList.get(1).suffix());
		assertEquals(retryTopicSuffix + "-2000", propertiesList.get(2).suffix());
		assertEquals(retryTopicSuffix + "-3000-0", propertiesList.get(3).suffix());
		assertEquals(retryTopicSuffix + "-3000-1", propertiesList.get(4).suffix());
		assertEquals(dltSuffix, propertiesList.get(5).suffix());
	}
}
