/*
 * Copyright 2018-present the original author or authors.
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
import java.util.Collection;
import java.util.stream.Stream;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.config.MultiMethodKafkaListenerEndpoint;
import org.springframework.kafka.support.EndpointHandlerMethod;
import org.springframework.kafka.support.EndpointHandlerMultiMethod;
import org.springframework.kafka.support.TopicPartitionOffset;

/**
 *
 * Creates the {@link EndpointCustomizer} that will be used by the {@link RetryTopicConfigurer}
 * to customize the main, retry and DLT endpoints.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @author Wang Zhiyang
 *
 * @since 2.7.2
 *
 * @see RetryTopicConfigurer
 * @see DestinationTopic.Properties
 * @see RetryTopicNamesProviderFactory.RetryTopicNamesProvider
 *
 */
public class EndpointCustomizerFactory {

	private static final int DEFAULT_PARTITION_FOR_MANUAL_ASSIGNMENT = 0;

	private final DestinationTopic.Properties destinationProperties;

	private final EndpointHandlerMethod beanMethod;

	private final BeanFactory beanFactory;

	private final RetryTopicNamesProviderFactory retryTopicNamesProviderFactory;

	public EndpointCustomizerFactory(DestinationTopic.Properties destinationProperties, EndpointHandlerMethod beanMethod,
							BeanFactory beanFactory, RetryTopicNamesProviderFactory retryTopicNamesProviderFactory) {

		this.destinationProperties = destinationProperties;
		this.beanMethod = beanMethod;
		this.beanFactory = beanFactory;
		this.retryTopicNamesProviderFactory = retryTopicNamesProviderFactory;
	}

	public final EndpointCustomizer<MethodKafkaListenerEndpoint<?, ?>> createEndpointCustomizer() {
		return addSuffixesAndMethod(this.destinationProperties);
	}

	/**
	 * Create MethodKafkaListenerEndpoint's EndpointCustomizer, but not support MultiMethodKafkaListenerEndpoint.
	 * Replace by {@link #addSuffixesAndMethod(DestinationTopic.Properties)}
	 * @param properties the destination-topic's properties.
	 * @param bean the bean.
	 * @param method the method.
	 * @return the endpoint customizer.
	 */
	@Deprecated(since = "3.2", forRemoval = true)
	@SuppressWarnings("rawtypes")
	protected EndpointCustomizer addSuffixesAndMethod(DestinationTopic.Properties properties, Object bean,
			Method method) {

		RetryTopicNamesProviderFactory.RetryTopicNamesProvider namesProvider =
				this.retryTopicNamesProviderFactory.createRetryTopicNamesProvider(properties);
		return endpoint -> {
			Collection<EndpointCustomizer.TopicNamesHolder> topics =
					customizeAndRegisterTopics(namesProvider, endpoint);
			configurationEndpoint(endpoint, namesProvider, properties, bean);
			endpoint.setMethod(method);
			return topics;
		};
	}

	/**
	 * Create MethodKafkaListenerEndpoint's EndpointCustomizer and support MultiMethodKafkaListenerEndpoint.
	 * @param properties the destination-topic's properties.
	 * @return the endpoint customizer.
	 * @since 3.2
	 */
	protected EndpointCustomizer<MethodKafkaListenerEndpoint<?, ?>> addSuffixesAndMethod(
			DestinationTopic.Properties properties) {

		RetryTopicNamesProviderFactory.RetryTopicNamesProvider namesProvider =
				this.retryTopicNamesProviderFactory.createRetryTopicNamesProvider(properties);
		return endpoint -> {
			Collection<EndpointCustomizer.TopicNamesHolder> topics =
					customizeAndRegisterTopics(namesProvider, endpoint);
			configurationEndpoint(endpoint, namesProvider, properties, this.beanMethod.resolveBean(this.beanFactory));
			if (endpoint instanceof MultiMethodKafkaListenerEndpoint<?, ?> multiMethodEndpoint
					&& this.beanMethod instanceof EndpointHandlerMultiMethod beanMultiMethod) {
				multiMethodEndpoint.setDefaultMethod(beanMultiMethod.getDefaultMethod());
				multiMethodEndpoint.setMethods(beanMultiMethod.getMethods());
			}
			else {
				endpoint.setMethod(this.beanMethod.getMethod());
			}
			return topics;
		};
	}

	private void configurationEndpoint(MethodKafkaListenerEndpoint<?, ?> endpoint,
			RetryTopicNamesProviderFactory.RetryTopicNamesProvider namesProvider,
			DestinationTopic.Properties properties, Object bean) {

		endpoint.setId(namesProvider.getEndpointId(endpoint));
		endpoint.setGroupId(namesProvider.getGroupId(endpoint));
		if (endpoint.getTopics().isEmpty() && endpoint.getTopicPartitionsToAssign() != null) {
			endpoint.setTopicPartitions(getTopicPartitions(properties, namesProvider,
					endpoint.getTopicPartitionsToAssign()));
		}
		else {
			endpoint.setTopics(endpoint.getTopics().stream()
					.map(namesProvider::getTopicName).toArray(String[]::new));
		}
		endpoint.setClientIdPrefix(namesProvider.getClientIdPrefix(endpoint));
		endpoint.setGroup(namesProvider.getGroup(endpoint));
		endpoint.setBean(bean);
		Boolean autoStartDltHandler = properties.autoStartDltHandler();
		if (autoStartDltHandler != null && properties.isDltTopic()) {
			endpoint.setAutoStartup(autoStartDltHandler);
		}
	}

	private static TopicPartitionOffset[] getTopicPartitions(DestinationTopic.Properties properties,
													RetryTopicNamesProviderFactory.RetryTopicNamesProvider namesProvider,
													@Nullable TopicPartitionOffset[] topicPartitionOffsets) {

		return Stream.of(topicPartitionOffsets)
				.map(tpo -> properties.isMainEndpoint()
						? getTPOForMainTopic(namesProvider, tpo)
						: getTPOForRetryTopics(properties, namesProvider, tpo))
				.toArray(TopicPartitionOffset[]::new);
	}

	private static TopicPartitionOffset getTPOForRetryTopics(DestinationTopic.Properties properties, RetryTopicNamesProviderFactory.RetryTopicNamesProvider namesProvider, TopicPartitionOffset tpo) {
		return new TopicPartitionOffset(namesProvider.getTopicName(tpo.getTopic()),
				tpo.getPartition() <= properties.numPartitions() ? tpo.getPartition() : DEFAULT_PARTITION_FOR_MANUAL_ASSIGNMENT,
				(Long) null);
	}

	private static TopicPartitionOffset getTPOForMainTopic(RetryTopicNamesProviderFactory.RetryTopicNamesProvider namesProvider, TopicPartitionOffset tpo) {
		TopicPartitionOffset newTpo = new TopicPartitionOffset(namesProvider.getTopicName(tpo.getTopic()),
				tpo.getPartition(), tpo.getOffset(), tpo.getPosition());
		newTpo.setRelativeToCurrent(tpo.isRelativeToCurrent());
		return newTpo;
	}

	protected Collection<EndpointCustomizer.TopicNamesHolder> customizeAndRegisterTopics(
			RetryTopicNamesProviderFactory.RetryTopicNamesProvider namesProvider,
			MethodKafkaListenerEndpoint<?, ?> endpoint) {

		return getTopics(endpoint)
				.stream()
				.map(topic -> new EndpointCustomizer.TopicNamesHolder(topic, namesProvider.getTopicName(topic)))
				.toList();
	}

	private Collection<String> getTopics(MethodKafkaListenerEndpoint<?, ?> endpoint) {
		Collection<String> topics = endpoint.getTopics();
		if (topics.isEmpty()) {
			@Nullable TopicPartitionOffset[] topicPartitionsToAssign = endpoint.getTopicPartitionsToAssign();
			if (topicPartitionsToAssign != null && topicPartitionsToAssign.length > 0) {
				topics = Arrays.stream(topicPartitionsToAssign)
						.map(TopicPartitionOffset::getTopic)
						.distinct()
						.toList();
			}
		}

		if (topics.isEmpty()) {
			throw new IllegalStateException(
					String.format("No topics were provided for RetryTopicConfiguration for method %s in class %s.",
							endpoint.getMethod().getName(), endpoint.getBean().getClass().getSimpleName()));
		}
		return topics;
	}
}
