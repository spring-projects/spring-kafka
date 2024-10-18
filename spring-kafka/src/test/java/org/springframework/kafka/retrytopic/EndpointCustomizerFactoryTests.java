/*
 * Copyright 2022 the original author or authors.
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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.support.EndpointHandlerMethod;
import org.springframework.kafka.support.TopicPartitionOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

/**
 * @author Tomaz Fernandes
 * @since 2.8.5
 */
@ExtendWith(MockitoExtension.class)
class EndpointCustomizerFactoryTests {

	@Mock
	private DestinationTopic.Properties properties;

	@Mock
	private EndpointHandlerMethod beanMethod;

	@Mock
	private BeanFactory beanFactory;

	@Mock
	private RetryTopicNamesProviderFactory retryTopicNamesProviderFactory;

	@Mock
	private MethodKafkaListenerEndpoint<?, ?> endpoint;

	private final String[] topics = {"myTopic1", "myTopic2"};

	private final Method method = EndpointCustomizerFactory.class.getDeclaredMethods()[0];

	@Test
	void shouldNotCustomizeEndpointForMainTopicWithTopics() {

		given(beanMethod.resolveBean(this.beanFactory)).willReturn(method);
		given(endpoint.getTopics()).willReturn(Arrays.asList(topics));
		given(properties.suffix()).willReturn("");
		RetryTopicNamesProviderFactory.RetryTopicNamesProvider provider =
				new SuffixingRetryTopicNamesProviderFactory().createRetryTopicNamesProvider(properties);
		given(retryTopicNamesProviderFactory.createRetryTopicNamesProvider(properties)).willReturn(provider);

		EndpointCustomizer endpointCustomizer = new EndpointCustomizerFactory(properties, beanMethod,
				beanFactory, retryTopicNamesProviderFactory).createEndpointCustomizer();

		List<EndpointCustomizer.TopicNamesHolder> holders =
				(List<EndpointCustomizer.TopicNamesHolder>) endpointCustomizer.customizeEndpointAndCollectTopics(endpoint);

		assertThat(holders).hasSize(2).element(0)
				.matches(assertMainTopic(0));
		assertThat(holders).element(1)
				.matches(assertMainTopic(1));

	}

	@Test
	void shouldNotCustomizeEndpointForMainTopicWithTPO() {

		given(beanMethod.resolveBean(this.beanFactory)).willReturn(method);
		given(properties.isMainEndpoint()).willReturn(true);
		given(properties.suffix()).willReturn("");
		RetryTopicNamesProviderFactory.RetryTopicNamesProvider provider =
				new SuffixingRetryTopicNamesProviderFactory().createRetryTopicNamesProvider(properties);
		given(retryTopicNamesProviderFactory.createRetryTopicNamesProvider(properties)).willReturn(provider);

		String testString = "testString";
		MethodKafkaListenerEndpoint<Object, Object> endpointTPO = new MethodKafkaListenerEndpoint<>();
		endpointTPO.setTopicPartitions(new TopicPartitionOffset(topics[0], 0, 0L),
				new TopicPartitionOffset(topics[1], 1, 1L));
		endpointTPO.setMethod(this.method);
		endpointTPO.setId(testString);
		endpointTPO.setClientIdPrefix(testString);
		endpointTPO.setGroup(testString);

		EndpointCustomizer endpointCustomizer = new EndpointCustomizerFactory(properties, beanMethod,
				beanFactory, retryTopicNamesProviderFactory).createEndpointCustomizer();

		List<EndpointCustomizer.TopicNamesHolder> holders =
				(List<EndpointCustomizer.TopicNamesHolder>) endpointCustomizer.customizeEndpointAndCollectTopics(endpointTPO);

		assertThat(holders).hasSize(2).element(0)
				.matches(assertMainTopic(0));
		assertThat(holders).element(1)
				.matches(assertMainTopic(1));

		assertThat(endpointTPO.getTopics())
				.isEmpty();

		TopicPartitionOffset[] topicPartitionsToAssign = endpointTPO.getTopicPartitionsToAssign();
		assertThat(topicPartitionsToAssign).hasSize(2);
		assertThat(equalsTopicPartitionOffset(topicPartitionsToAssign[0],
				new TopicPartitionOffset(topics[0], 0, 0L))).isTrue();
		assertThat(equalsTopicPartitionOffset(topicPartitionsToAssign[1],
				new TopicPartitionOffset(topics[1], 1, 1L))).isTrue();

	}

	private Predicate<EndpointCustomizer.TopicNamesHolder> assertMainTopic(int index) {
		return holder -> holder.getCustomizedTopic().equals(topics[index])
				&& holder.getMainTopic().equals(topics[index]);
	}

	@Test
	void shouldCustomizeEndpointForRetryTopic() {

		MethodKafkaListenerEndpoint<Object, Object> endpoint = new MethodKafkaListenerEndpoint<>();
		String testString = "testString";
		endpoint.setTopics(this.topics);
		endpoint.setMethod(this.method);
		endpoint.setId(testString);
		endpoint.setClientIdPrefix(testString);
		endpoint.setGroup(testString);

		MethodKafkaListenerEndpoint<Object, Object> endpointTPO = new MethodKafkaListenerEndpoint<>();
		endpointTPO.setTopicPartitions(new TopicPartitionOffset(topics[0], 0, 0L),
				new TopicPartitionOffset(topics[1], 1, 1L));
		endpointTPO.setMethod(this.method);
		endpointTPO.setId(testString);
		endpointTPO.setClientIdPrefix(testString);
		endpointTPO.setGroup(testString);

		String suffix = "-retry";
		given(beanMethod.resolveBean(this.beanFactory)).willReturn(method);
		given(properties.isMainEndpoint()).willReturn(false);
		given(properties.suffix()).willReturn(suffix);
		given(properties.numPartitions()).willReturn(2);

		RetryTopicNamesProviderFactory.RetryTopicNamesProvider provider =
				new SuffixingRetryTopicNamesProviderFactory().createRetryTopicNamesProvider(properties);
		given(retryTopicNamesProviderFactory.createRetryTopicNamesProvider(properties)).willReturn(provider);

		EndpointCustomizer endpointCustomizer = new EndpointCustomizerFactory(properties, beanMethod,
				beanFactory, retryTopicNamesProviderFactory).createEndpointCustomizer();

		List<EndpointCustomizer.TopicNamesHolder> holders =
				(List<EndpointCustomizer.TopicNamesHolder>) endpointCustomizer.customizeEndpointAndCollectTopics(endpoint);

		String topic1WithSuffix = topics[0] + suffix;
		String topic2WithSuffix = topics[1] + suffix;
		assertThat(holders).hasSize(2).element(0)
				.matches(holder -> holder.getMainTopic().equals(topics[0])
						&& holder.getCustomizedTopic().equals(topic1WithSuffix));
		assertThat(holders).hasSize(2).element(1)
				.matches(holder -> holder.getMainTopic().equals(topics[1])
						&& holder.getCustomizedTopic().equals(topic2WithSuffix));

		String testStringSuffix = testString + suffix;

		assertThat(endpoint.getTopics())
				.contains(topic1WithSuffix, topic2WithSuffix);
		assertThat(endpoint.getId())
				.isEqualTo(testStringSuffix);
		assertThat(endpoint.getClientIdPrefix())
				.isEqualTo(testStringSuffix);
		assertThat(endpoint.getGroup())
				.isEqualTo(testStringSuffix);
		assertThat(endpoint.getTopicPartitionsToAssign()).isEmpty();

		List<EndpointCustomizer.TopicNamesHolder> holdersTPO =
				(List<EndpointCustomizer.TopicNamesHolder>) endpointCustomizer.customizeEndpointAndCollectTopics(endpointTPO);

		assertThat(holdersTPO).hasSize(2).element(0)
				.matches(holder -> holder.getMainTopic().equals(topics[0])
						&& holder.getCustomizedTopic().equals(topic1WithSuffix));
		assertThat(holdersTPO).hasSize(2).element(1)
				.matches(holder -> holder.getMainTopic().equals(topics[1])
						&& holder.getCustomizedTopic().equals(topic2WithSuffix));

		assertThat(endpointTPO.getTopics())
				.isEmpty();

		TopicPartitionOffset[] topicPartitionsToAssign = endpointTPO.getTopicPartitionsToAssign();
		assertThat(topicPartitionsToAssign).hasSize(2);
		assertThat(equalsTopicPartitionOffset(topicPartitionsToAssign[0],
				new TopicPartitionOffset(topic1WithSuffix, 0, (Long) null))).isTrue();
		assertThat(equalsTopicPartitionOffset(topicPartitionsToAssign[1],
				new TopicPartitionOffset(topic2WithSuffix, 1, (Long) null))).isTrue();

		assertThat(endpointTPO.getId())
				.isEqualTo(testStringSuffix);
		assertThat(endpointTPO.getClientIdPrefix())
				.isEqualTo(testStringSuffix);
		assertThat(endpointTPO.getGroup())
				.isEqualTo(testStringSuffix);
	}

	private boolean equalsTopicPartitionOffset(TopicPartitionOffset tpo1, TopicPartitionOffset tpo2) {
		return tpo1.getTopicPartition().equals(tpo2.getTopicPartition()) &&
				((tpo1.getOffset() == null && tpo2.getOffset() == null) ||
						(tpo1.getOffset() != null && tpo1.getOffset().equals(tpo2.getOffset())));

	}
}
