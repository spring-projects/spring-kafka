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

package org.springframework.kafka.annotation;

import java.lang.reflect.Method;
import java.util.Arrays;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.adapter.AdapterUtils;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurer;
import org.springframework.kafka.retrytopic.RetryTopicInternalBeanNames;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.ExponentialRandomBackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.backoff.SleepingBackOffPolicy;
import org.springframework.retry.backoff.UniformRandomBackOffPolicy;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * Processes the provided {@link RetryableTopic} annotation
 * returning an {@link RetryTopicConfiguration}.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public class RetryableTopicAnnotationProcessor {

	private static final SpelExpressionParser PARSER;

	private final BeanFactory beanFactory;

	static {
		PARSER = new SpelExpressionParser();
	}

	public RetryableTopicAnnotationProcessor(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}

	public RetryTopicConfiguration processAnnotation(String[] topics, Method method, RetryableTopic annotation,
			Object bean) {

		return RetryTopicConfiguration.builder()
				.maxAttempts(annotation.attempts())
				.customBackoff(createBackoffFromAnnotation(annotation.backoff(), this.beanFactory))
				.retryTopicSuffix(annotation.retryTopicSuffix())
				.dltSuffix(annotation.dltTopicSuffix())
				.dltHandlerMethod(getDltProcessor(method, bean))
				.includeTopics(Arrays.asList(topics))
				.listenerFactory(annotation.listenerContainerFactory())
				.autoCreateTopics(annotation.autoCreateTopics(), annotation.numPartitions(), annotation.replicationFactor())
				.retryOn(Arrays.asList(annotation.include()))
				.notRetryOn(Arrays.asList(annotation.exclude()))
				.traversingCauses(annotation.traversingCauses())
				.useSingleTopicForFixedDelays(annotation.fixedDelayTopicStrategy())
				.dltProcessingFailureStrategy(annotation.dltStrategy())
				.setTopicSuffixingStrategy(annotation.topicSuffixingStrategy())
				.timeoutAfter(annotation.timeout())
				.create(getKafkaTemplate(annotation.kafkaTemplate(), topics));
	}

	private SleepingBackOffPolicy<?> createBackoffFromAnnotation(Backoff backoff, BeanFactory beanFactory) { // NOSONAR
		StandardEvaluationContext evaluationContext = new StandardEvaluationContext();
		evaluationContext.setBeanResolver(new BeanFactoryResolver(beanFactory));

		// Code from Spring Retry
		Long min = backoff.delay() == 0 ? backoff.value() : backoff.delay();
		if (StringUtils.hasText(backoff.delayExpression())) {
			min = PARSER.parseExpression(resolve(backoff.delayExpression(), beanFactory), AdapterUtils.PARSER_CONTEXT)
					.getValue(evaluationContext, Long.class);
		}
		Long max = backoff.maxDelay();
		if (StringUtils.hasText(backoff.maxDelayExpression())) {
			max = PARSER
					.parseExpression(resolve(backoff.maxDelayExpression(), beanFactory), AdapterUtils.PARSER_CONTEXT)
					.getValue(evaluationContext, Long.class);
		}
		Double multiplier = backoff.multiplier();
		if (StringUtils.hasText(backoff.multiplierExpression())) {
			multiplier = PARSER
					.parseExpression(resolve(backoff.multiplierExpression(), beanFactory), AdapterUtils.PARSER_CONTEXT)
					.getValue(evaluationContext, Double.class);
		}
		if (multiplier != null && multiplier > 0) {
			ExponentialBackOffPolicy policy = new ExponentialBackOffPolicy();
			if (backoff.random()) {
				policy = new ExponentialRandomBackOffPolicy();
			}
			policy.setInitialInterval(min);
			policy.setMultiplier(multiplier);
			policy.setMaxInterval(max > min ? max : ExponentialBackOffPolicy.DEFAULT_MAX_INTERVAL);
			return policy;
		}
		if (max != null && min != null && max > min) {
			UniformRandomBackOffPolicy policy = new UniformRandomBackOffPolicy();
			policy.setMinBackOffPeriod(min);
			policy.setMaxBackOffPeriod(max);
			return policy;
		}
		FixedBackOffPolicy policy = new FixedBackOffPolicy();
		if (min != null) {
			policy.setBackOffPeriod(min);
		}
		return policy;
	}

	private String resolve(String value, BeanFactory beanFactory) {
		if (beanFactory instanceof ConfigurableBeanFactory) {
			return ((ConfigurableBeanFactory) beanFactory).resolveEmbeddedValue(value);
		}
		return value;
	}

	private RetryTopicConfigurer.EndpointHandlerMethod getDltProcessor(Method listenerMethod, Object bean) {
		Class<?> declaringClass = listenerMethod.getDeclaringClass();
		return Arrays.stream(ReflectionUtils.getDeclaredMethods(declaringClass))
				.filter(method -> AnnotationUtils.findAnnotation(method, DltHandler.class) != null)
				.map(method -> RetryTopicConfigurer.createHandlerMethodWith(bean, method))
				.findFirst()
				.orElse(RetryTopicConfigurer.DEFAULT_DLT_HANDLER);
	}

	private KafkaOperations<?, ?> getKafkaTemplate(String kafkaTemplateName, String[] topics) {
		if (StringUtils.hasText(kafkaTemplateName)) {
			Assert.state(this.beanFactory != null, "BeanFactory must be set to obtain kafka template by bean name");
			try {
				return this.beanFactory.getBean(kafkaTemplateName, KafkaOperations.class);
			}
			catch (NoSuchBeanDefinitionException ex) {
				throw new BeanInitializationException("Could not register Kafka listener endpoint for topics "
						+ Arrays.asList(topics) + ", no " + KafkaOperations.class.getSimpleName()
						+ " with id '" + kafkaTemplateName + "' was found in the application context", ex);
			}
		}
		try {
			return this.beanFactory.getBean(RetryTopicInternalBeanNames.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class);
		}
		catch (NoSuchBeanDefinitionException ex) {
			throw new BeanInitializationException("Could not find a KafkaTemplate to configure the retry topics.", ex);
		}
	}
}
