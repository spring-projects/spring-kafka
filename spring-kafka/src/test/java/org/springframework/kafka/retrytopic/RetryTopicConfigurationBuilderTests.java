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

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * @author Tomaz Fernandes
 * @since 2.7
 */
@ExtendWith(MockitoExtension.class)
class RetryTopicConfigurationBuilderTests {

	@Mock
	private KafkaOperations<?, ?> kafkaOperations;

	@Mock
	private ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory;

	@Test
	void shouldExcludeTopics() {

		// setup
		RetryTopicConfigurationBuilder builder = new RetryTopicConfigurationBuilder();
		String topic1 = "topic1";
		String topic2 = "topic2";
		String[] topicNames = {topic1, topic2};
		List<String> topicNamesList = Arrays.asList(topicNames);

		//when
		builder.excludeTopics(topicNamesList);
		RetryTopicConfiguration configuration = builder.create(kafkaOperations);

		// then
		assertThat(configuration.hasConfigurationForTopics(topicNames)).isFalse();
	}

	@Test
	void shouldSetFixedBackOffPolicy() {

		// setup
		RetryTopicConfigurationBuilder builder = new RetryTopicConfigurationBuilder();
		builder.fixedBackOff(1000);

		//when
		RetryTopicConfiguration configuration = builder.create(kafkaOperations);

		// then
		List<DestinationTopic.Properties> destinationTopicProperties = configuration.getDestinationTopicProperties();
		assertThat(destinationTopicProperties.get(0).delay()).isEqualTo(0);
		assertThat(destinationTopicProperties.get(1).delay()).isEqualTo(1000);
		assertThat(destinationTopicProperties.get(2).delay()).isEqualTo(1000);
		assertThat(destinationTopicProperties.get(3).delay()).isEqualTo(0);
	}

	@Test
	void shouldSetNoBackoffPolicy() {

		// setup
		RetryTopicConfigurationBuilder builder = new RetryTopicConfigurationBuilder();
		builder.noBackoff();

		//when
		RetryTopicConfiguration configuration = builder.create(kafkaOperations);

		// then
		List<DestinationTopic.Properties> destinationTopicProperties = configuration.getDestinationTopicProperties();
		assertThat(destinationTopicProperties.get(0).delay()).isEqualTo(0);
		assertThat(destinationTopicProperties.get(1).delay()).isEqualTo(0);
		assertThat(destinationTopicProperties.get(2).delay()).isEqualTo(0);
		assertThat(destinationTopicProperties.get(3).delay()).isEqualTo(0);


	}

	@Test
	void shouldSetUniformRandomBackOff() {

		// setup
		RetryTopicConfigurationBuilder builder = new RetryTopicConfigurationBuilder();
		int minInterval = 1000;
		int maxInterval = 10000;
		builder.uniformRandomBackoff(minInterval, maxInterval);

		//when
		RetryTopicConfiguration configuration = builder.create(kafkaOperations);

		// then
		List<DestinationTopic.Properties> destinationTopicProperties = configuration.getDestinationTopicProperties();
		assertThat(destinationTopicProperties.get(0).delay()).isEqualTo(0);
		assertThat(minInterval < destinationTopicProperties.get(1).delay()).isTrue();
		assertThat(destinationTopicProperties.get(1).delay() < maxInterval).isTrue();
		assertThat(minInterval < destinationTopicProperties.get(2).delay()).isTrue();
		assertThat(destinationTopicProperties.get(2).delay() < maxInterval).isTrue();
		assertThat(destinationTopicProperties.get(3).delay()).isEqualTo(0);
	}

	@Test
	void shouldRetryOn() {
		// setup
		RetryTopicConfigurationBuilder builder = new RetryTopicConfigurationBuilder();

		// when
		RetryTopicConfiguration configuration = builder.retryOn(IllegalArgumentException.class).create(kafkaOperations);

		// then
		DestinationTopic destinationTopic = new DestinationTopic("",
				configuration.getDestinationTopicProperties().get(0));
		assertThat(destinationTopic.shouldRetryOn(0, new IllegalArgumentException())).isTrue();
		assertThat(destinationTopic.shouldRetryOn(0, new IllegalStateException())).isFalse();
	}

	@Test
	void shouldNotRetryOn() {

		// setup
		RetryTopicConfigurationBuilder builder = new RetryTopicConfigurationBuilder();

		//when
		RetryTopicConfiguration configuration = builder.notRetryOn(IllegalArgumentException.class).create(kafkaOperations);

		// then
		DestinationTopic destinationTopic = new DestinationTopic("",
				configuration.getDestinationTopicProperties().get(0));
		assertThat(destinationTopic.shouldRetryOn(0, new IllegalArgumentException())).isFalse();
		assertThat(destinationTopic.shouldRetryOn(0, new IllegalStateException())).isTrue();
	}

	@Test
	void shouldSetGivenFactory() {

		// setup
		String factoryName = "factoryName";
		RetryTopicConfigurationBuilder builder = new RetryTopicConfigurationBuilder();
		RetryTopicConfiguration configuration = builder
				.listenerFactory(containerFactory)
				.listenerFactory(factoryName)
				.create(kafkaOperations);

		ListenerContainerFactoryResolver.Configuration config = configuration.forContainerFactoryResolver();
		Object factoryInstance = ReflectionTestUtils.getField(config, "factoryFromRetryTopicConfiguration");
		Object listenerContainerFactoryName = ReflectionTestUtils.getField(config, "listenerContainerFactoryName");

		assertThat(factoryInstance).isEqualTo(containerFactory);
		assertThat(listenerContainerFactoryName).isEqualTo(factoryName);

	}

	@Test
	void shouldSetNotAutoCreateTopics() {

		// setup
		RetryTopicConfigurationBuilder builder = new RetryTopicConfigurationBuilder();
		RetryTopicConfiguration configuration = builder
				.doNotAutoCreateRetryTopics()
				.create(kafkaOperations);

		RetryTopicConfiguration.TopicCreation config = configuration.forKafkaTopicAutoCreation();
		assertThat(config.shouldCreateTopics()).isFalse();
	}
}
