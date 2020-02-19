/*
 * Copyright 2020 the original author or authors.
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

package org.springframework.kafka.support;

import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * A property resolver that overrides
 * values provided by {@link KafkaListener}.
 *
 * @author Aliaksandr Rahavoi
 *
 * @see KafkaListener
 */
public class KafkaEndpointPropertyResolver {

	private static final String GENERATED_ID_PREFIX = "org.springframework.kafka.KafkaListenerEndpointContainer#";

	private final AtomicInteger counter = new AtomicInteger();

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));

	private BeanFactory beanFactory;

	private BeanExpressionResolver resolver;

	private BeanExpressionContext expressionContext;

	/**
	 * Set the {@link BeanFactory} to be used for resolving
	 * embedded values.
	 * @param beanFactory the {@link BeanFactory} instance.
	 */
	public void setBeanFactory(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}

	/**
	 * Set the {@link BeanExpressionResolver} that will resolve value
	 * through evaluating it as an expression.
	 * @param resolver the {@link BeanExpressionResolver} instance.
	 */
	public void setResolver(BeanExpressionResolver resolver) {
		this.resolver = resolver;
	}

	/**
	 * Set the {@link BeanExpressionContext} for evaluating
	 * an expression within a bean definition.
	 * @param expressionContext the {@link BeanExpressionContext} instance.
	 */
	public void setExpressionContext(BeanExpressionContext expressionContext) {
		this.expressionContext = expressionContext;
	}

	public String getEndpointId(KafkaListener kafkaListener) {
		if (StringUtils.hasText(kafkaListener.id())) {
			return resolveExpressionAsString(kafkaListener.id(), "id");
		}
		else {
			return GENERATED_ID_PREFIX + this.counter.getAndIncrement();
		}
	}

	public String getEndpointGroupId(KafkaListener kafkaListener, String id) {
		String groupId = null;
		if (StringUtils.hasText(kafkaListener.groupId())) {
			groupId = resolveExpressionAsString(kafkaListener.groupId(), "groupId");
		}
		if (groupId == null && kafkaListener.idIsGroup() && StringUtils.hasText(kafkaListener.id())) {
			groupId = id;
		}
		return groupId;
	}

	public TopicPartitionOffset[] resolveTopicPartitions(KafkaListener kafkaListener) {
		TopicPartition[] topicPartitions = kafkaListener.topicPartitions();
		List<TopicPartitionOffset> result = new ArrayList<>();
		if (topicPartitions.length > 0) {
			for (TopicPartition topicPartition : topicPartitions) {
				result.addAll(resolveTopicPartitionsList(topicPartition));
			}
		}
		return result.toArray(new TopicPartitionOffset[0]);
	}

	public String[] resolveTopics(KafkaListener kafkaListener) {
		String[] topics = kafkaListener.topics();
		List<String> result = new ArrayList<>();
		if (topics.length > 0) {
			for (String topic1 : topics) {
				Object topic = resolveExpression(topic1);
				resolveAsString(topic, result);
			}
		}
		return result.toArray(new String[0]);
	}

	public Pattern resolvePattern(KafkaListener kafkaListener) {
		Pattern pattern = null;
		String text = kafkaListener.topicPattern();
		if (StringUtils.hasText(text)) {
			Object resolved = resolveExpression(text);
			if (resolved instanceof Pattern) {
				pattern = (Pattern) resolved;
			}
			else if (resolved instanceof String) {
				pattern = Pattern.compile((String) resolved);
			}
			else if (resolved != null) {
				throw new IllegalStateException(
						"topicPattern must resolve to a Pattern or String, not " + resolved.getClass());
			}
		}
		return pattern;
	}

	public String resolveExpressionAsString(String value, String attribute) {
		Object resolved = resolveExpression(value);
		if (resolved instanceof String) {
			return (String) resolved;
		}
		else if (resolved != null) {
			throw new IllegalStateException("The [" + attribute + "] must resolve to a String. "
					+ "Resolved to [" + resolved.getClass() + "] for [" + value + "]");
		}
		return null;
	}

	public Object resolveExpression(String value) {
		return this.resolver.evaluate(resolve(value), this.expressionContext);
	}

	public Integer resolveExpressionAsInteger(String value, String attribute) {
		Object resolved = resolveExpression(value);
		Integer result = null;
		if (resolved instanceof String) {
			result = Integer.parseInt((String) resolved);
		}
		else if (resolved instanceof Number) {
			result = ((Number) resolved).intValue();
		}
		else if (resolved != null) {
			throw new IllegalStateException(
					"The [" + attribute + "] must resolve to an Number or a String that can be parsed as an Integer. "
							+ "Resolved to [" + resolved.getClass() + "] for [" + value + "]");
		}
		return result;
	}

	public Boolean resolveExpressionAsBoolean(String value, String attribute) {
		Object resolved = resolveExpression(value);
		Boolean result = null;
		if (resolved instanceof Boolean) {
			result = (Boolean) resolved;
		}
		else if (resolved instanceof String) {
			result = Boolean.parseBoolean((String) resolved);
		}
		else if (resolved != null) {
			throw new IllegalStateException(
					"The [" + attribute + "] must resolve to a Boolean or a String that can be parsed as a Boolean. "
							+ "Resolved to [" + resolved.getClass() + "] for [" + value + "]");
		}
		return result;
	}

	public void resolveKafkaProperties(MethodKafkaListenerEndpoint<?, ?> endpoint, String[] propertyStrings) {
		if (propertyStrings.length > 0) {
			Properties properties = new Properties();
			for (String property : propertyStrings) {
				String value = resolveExpressionAsString(property, "property");
				if (value != null) {
					try {
						properties.load(new StringReader(value));
					}
					catch (IOException e) {
						this.logger.error(e, () -> "Failed to load property " + property + ", continuing...");
					}
				}
			}
			endpoint.setConsumerProperties(properties);
		}
	}

	/**
	 * Resolve the specified value if possible.
	 * @param value the value to resolve
	 * @return the resolved value
	 * @see ConfigurableBeanFactory#resolveEmbeddedValue
	 */
	public String resolve(String value) {
		if (this.beanFactory instanceof ConfigurableBeanFactory) {
			return ((ConfigurableBeanFactory) this.beanFactory).resolveEmbeddedValue(value);
		}
		return value;
	}

	@SuppressWarnings("unchecked")
	private void resolveAsString(Object resolvedValue, List<String> result) {
		if (resolvedValue instanceof String[]) {
			for (Object object : (String[]) resolvedValue) {
				resolveAsString(object, result);
			}
		}
		else if (resolvedValue instanceof String) {
			result.add((String) resolvedValue);
		}
		else if (resolvedValue instanceof Iterable) {
			for (Object object : (Iterable<Object>) resolvedValue) {
				resolveAsString(object, result);
			}
		}
		else {
			throw new IllegalArgumentException(String.format(
					"@KafKaListener can't resolve '%s' as a String", resolvedValue));
		}
	}

	private List<TopicPartitionOffset> resolveTopicPartitionsList(TopicPartition topicPartition) {
		Object topic = resolveExpression(topicPartition.topic());
		Assert.state(topic instanceof String,
				() -> "topic in @TopicPartition must resolve to a String, not " + topic.getClass());
		Assert.state(StringUtils.hasText((String) topic), "topic in @TopicPartition must not be empty");
		String[] partitions = topicPartition.partitions();
		PartitionOffset[] partitionOffsets = topicPartition.partitionOffsets();
		Assert.state(partitions.length > 0 || partitionOffsets.length > 0,
				() -> "At least one 'partition' or 'partitionOffset' required in @TopicPartition for topic '" + topic + "'");
		List<TopicPartitionOffset> result = new ArrayList<>();
		for (String partition : partitions) {
			resolvePartitionAsInteger((String) topic, resolveExpression(partition), result);
		}

		for (PartitionOffset partitionOffset : partitionOffsets) {
			TopicPartitionOffset topicPartitionOffset =
					new TopicPartitionOffset((String) topic,
							resolvePartition(topic, partitionOffset),
							resolveInitialOffset(topic, partitionOffset),
							isRelative(topic, partitionOffset));
			if (!result.contains(topicPartitionOffset)) {
				result.add(topicPartitionOffset);
			}
			else {
				throw new IllegalArgumentException(
						String.format("@TopicPartition can't have the same partition configuration twice: [%s]",
								topicPartitionOffset));
			}
		}
		return result;
	}

	private Integer resolvePartition(Object topic, PartitionOffset partitionOffset) {
		Object partitionValue = resolveExpression(partitionOffset.partition());
		Integer partition;
		if (partitionValue instanceof String) {
			Assert.state(StringUtils.hasText((String) partitionValue),
					() -> "partition in @PartitionOffset for topic '" + topic + "' cannot be empty");
			partition = Integer.valueOf((String) partitionValue);
		}
		else if (partitionValue instanceof Integer) {
			partition = (Integer) partitionValue;
		}
		else {
			throw new IllegalArgumentException(String.format(
					"@PartitionOffset for topic '%s' can't resolve '%s' as an Integer or String, resolved to '%s'",
					topic, partitionOffset.partition(), partitionValue.getClass()));
		}
		return partition;
	}

	private Long resolveInitialOffset(Object topic, PartitionOffset partitionOffset) {
		Object initialOffsetValue = resolveExpression(partitionOffset.initialOffset());
		Long initialOffset;
		if (initialOffsetValue instanceof String) {
			Assert.state(StringUtils.hasText((String) initialOffsetValue),
					() -> "'initialOffset' in @PartitionOffset for topic '" + topic + "' cannot be empty");
			initialOffset = Long.valueOf((String) initialOffsetValue);
		}
		else if (initialOffsetValue instanceof Long) {
			initialOffset = (Long) initialOffsetValue;
		}
		else {
			throw new IllegalArgumentException(String.format(
					"@PartitionOffset for topic '%s' can't resolve '%s' as a Long or String, resolved to '%s'",
					topic, partitionOffset.initialOffset(), initialOffsetValue.getClass()));
		}
		return initialOffset;
	}

	private boolean isRelative(Object topic, PartitionOffset partitionOffset) {
		Object relativeToCurrentValue = resolveExpression(partitionOffset.relativeToCurrent());
		Boolean relativeToCurrent;
		if (relativeToCurrentValue instanceof String) {
			relativeToCurrent = Boolean.valueOf((String) relativeToCurrentValue);
		}
		else if (relativeToCurrentValue instanceof Boolean) {
			relativeToCurrent = (Boolean) relativeToCurrentValue;
		}
		else {
			throw new IllegalArgumentException(String.format(
					"@PartitionOffset for topic '%s' can't resolve '%s' as a Boolean or String, resolved to '%s'",
					topic, partitionOffset.relativeToCurrent(), relativeToCurrentValue.getClass()));
		}
		return relativeToCurrent;
	}

	@SuppressWarnings("unchecked")
	private void resolvePartitionAsInteger(String topic, Object resolvedValue,
										   List<TopicPartitionOffset> result) {
		if (resolvedValue instanceof String[]) {
			for (Object object : (String[]) resolvedValue) {
				resolvePartitionAsInteger(topic, object, result);
			}
		}
		else if (resolvedValue instanceof String) {
			Assert.state(StringUtils.hasText((String) resolvedValue),
					() -> "partition in @TopicPartition for topic '" + topic + "' cannot be empty");
			result.add(new TopicPartitionOffset(topic, Integer.valueOf((String) resolvedValue)));
		}
		else if (resolvedValue instanceof Integer[]) {
			for (Integer partition : (Integer[]) resolvedValue) {
				result.add(new TopicPartitionOffset(topic, partition));
			}
		}
		else if (resolvedValue instanceof Integer) {
			result.add(new TopicPartitionOffset(topic, (Integer) resolvedValue));
		}
		else if (resolvedValue instanceof Iterable) {
			for (Object object : (Iterable<Object>) resolvedValue) {
				resolvePartitionAsInteger(topic, object, result);
			}
		}
		else {
			throw new IllegalArgumentException(String.format(
					"@KafKaListener for topic '%s' can't resolve '%s' as an Integer or String", topic, resolvedValue));
		}
	}
}
