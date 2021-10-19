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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.context.expression.StandardBeanExpressionResolver;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.retrytopic.EndpointHandlerMethod;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.retrytopic.RetryTopicConfigurer;
import org.springframework.kafka.retrytopic.RetryTopicConstants;
import org.springframework.kafka.retrytopic.RetryTopicInternalBeanNames;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.ExponentialRandomBackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.backoff.SleepingBackOffPolicy;
import org.springframework.retry.backoff.UniformRandomBackOffPolicy;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * Processes the provided {@link RetryableTopic} annotation
 * returning an {@link RetryTopicConfiguration}.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.7
 *
 */
public class RetryableTopicAnnotationProcessor {

	private static final String NULL = "null";

	private static final String THE_OSQ = "The [";

	private static final String RESOLVED_TO_OSQ = "Resolved to [";

	private static final String CSQ = "]";

	private static final String CSQ_FOR_OSQ = "] for [";

	private final BeanFactory beanFactory;

	private final BeanExpressionResolver resolver;

	private final BeanExpressionContext expressionContext;

	private static final String DEFAULT_SPRING_BOOT_KAFKA_TEMPLATE_NAME = "kafkaTemplate";

	/**
	 * Construct an instance using the provided parameters and default resolver,
	 * expression context.
	 * @param beanFactory the bean factory.
	 */
	public RetryableTopicAnnotationProcessor(BeanFactory beanFactory) {
		this(beanFactory, new StandardBeanExpressionResolver(), beanFactory instanceof ConfigurableBeanFactory
				? new BeanExpressionContext((ConfigurableBeanFactory) beanFactory, null)
				: null); // NOSONAR
	}

	/**
	 * Construct an instance using the provided parameters.
	 * @param beanFactory the bean factory.
	 * @param resolver the bean expression resolver.
	 * @param expressionContext the bean expression context.
	 */
	public RetryableTopicAnnotationProcessor(BeanFactory beanFactory, BeanExpressionResolver resolver,
			BeanExpressionContext expressionContext) {

		this.beanFactory = beanFactory;
		this.resolver = resolver;
		this.expressionContext = expressionContext;
	}

	public RetryTopicConfiguration processAnnotation(String[] topics, Method method, RetryableTopic annotation,
			Object bean) {

		Long resolvedTimeout = resolveExpressionAsLong(annotation.timeout(), "timeout", false);
		long timeout = RetryTopicConstants.NOT_SET;
		if (resolvedTimeout != null) {
			timeout = resolvedTimeout;
		}
		List<Class<? extends Throwable>> includes = resolveClasses(annotation.include(), annotation.includeNames(),
				"include");
		List<Class<? extends Throwable>> excludes = resolveClasses(annotation.exclude(), annotation.excludeNames(),
				"exclude");
		boolean traverse = false;
		if (StringUtils.hasText(annotation.traversingCauses())) {
			Boolean traverseResolved = resolveExpressionAsBoolean(annotation.traversingCauses(), "traversingCauses");
			if (traverseResolved != null) {
				traverse = traverseResolved;
			}
			else {
				traverse = includes.size() > 0 || excludes.size() > 0;
			}
		}
		Boolean autoStartDlt = null;
		if (StringUtils.hasText(annotation.autoStartDltHandler())) {
			autoStartDlt = resolveExpressionAsBoolean(annotation.autoStartDltHandler(), "autoStartDltContainer");
		}
		return RetryTopicConfigurationBuilder.newInstance()
				.maxAttempts(resolveExpressionAsInteger(annotation.attempts(), "attempts", true))
				.customBackoff(createBackoffFromAnnotation(annotation.backoff(), this.beanFactory))
				.retryTopicSuffix(resolveExpressionAsString(annotation.retryTopicSuffix(), "retryTopicSuffix"))
				.dltSuffix(resolveExpressionAsString(annotation.dltTopicSuffix(), "dltTopicSuffix"))
				.dltHandlerMethod(getDltProcessor(method, bean))
				.includeTopics(Arrays.asList(topics))
				.listenerFactory(annotation.listenerContainerFactory())
				.autoCreateTopics(resolveExpressionAsBoolean(annotation.autoCreateTopics(), "autoCreateTopics"),
						resolveExpressionAsInteger(annotation.numPartitions(), "numPartitions", true),
						resolveExpressionAsShort(annotation.replicationFactor(), "replicationFactor", true))
				.retryOn(includes)
				.notRetryOn(excludes)
				.traversingCauses(traverse)
				.useSingleTopicForFixedDelays(annotation.fixedDelayTopicStrategy())
				.dltProcessingFailureStrategy(annotation.dltStrategy())
				.autoStartDltHandler(autoStartDlt)
				.setTopicSuffixingStrategy(annotation.topicSuffixingStrategy())
				.timeoutAfter(timeout)
				.create(getKafkaTemplate(annotation.kafkaTemplate(), topics));
	}

	private SleepingBackOffPolicy<?> createBackoffFromAnnotation(Backoff backoff, BeanFactory beanFactory) { // NOSONAR
		StandardEvaluationContext evaluationContext = new StandardEvaluationContext();
		evaluationContext.setBeanResolver(new BeanFactoryResolver(beanFactory));

		// Code from Spring Retry
		Long min = backoff.delay() == 0 ? backoff.value() : backoff.delay();
		if (StringUtils.hasText(backoff.delayExpression())) {
			min = resolveExpressionAsLong(backoff.delayExpression(), "delayExpression", true);
		}
		Long max = backoff.maxDelay();
		if (StringUtils.hasText(backoff.maxDelayExpression())) {
			max = resolveExpressionAsLong(backoff.maxDelayExpression(), "maxDelayExpression", true);
		}
		Double multiplier = backoff.multiplier();
		if (StringUtils.hasText(backoff.multiplierExpression())) {
			multiplier = resolveExpressionAsDouble(backoff.multiplierExpression(), "multiplierExpression", true);
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

	private EndpointHandlerMethod getDltProcessor(Method listenerMethod, Object bean) {
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
			return this.beanFactory.getBean(RetryTopicInternalBeanNames.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME,
					KafkaOperations.class);
		}
		catch (NoSuchBeanDefinitionException ex) {
			try {
				return this.beanFactory.getBean(DEFAULT_SPRING_BOOT_KAFKA_TEMPLATE_NAME, KafkaOperations.class);
			}
			catch (NoSuchBeanDefinitionException exc) {
				exc.addSuppressed(ex);
				throw new BeanInitializationException("Could not find a KafkaTemplate to configure the retry topics.", // NOSONAR (lost stack trace)
						exc);
			}
		}
	}

	private String resolveExpressionAsString(String value, String attribute) {
		Object resolved = resolveExpression(value);
		if (resolved instanceof String) {
			return (String) resolved;
		}
		else if (resolved != null) {
			throw new IllegalStateException(THE_OSQ + attribute + "] must resolve to a String. "
					+ RESOLVED_TO_OSQ + resolved.getClass() + CSQ_FOR_OSQ + value + CSQ);
		}
		return null;
	}

	private Integer resolveExpressionAsInteger(String value, String attribute, boolean required) {
		Object resolved = resolveExpression(value);
		Integer result = null;
		if (resolved instanceof String) {
			if (!required && !StringUtils.hasText((String) resolved)) {
				result = null;
			}
			else {
				result = Integer.parseInt((String) resolved);
			}
		}
		else if (resolved instanceof Number) {
			result = ((Number) resolved).intValue();
		}
		else if (resolved != null || required) {
			throw new IllegalStateException(
					THE_OSQ + attribute + "] must resolve to an Number or a String that can be parsed as an Integer. "
							+ RESOLVED_TO_OSQ + (resolved == null ? NULL : resolved.getClass())
									+ CSQ_FOR_OSQ + value + CSQ);
		}
		return result;
	}

	private Short resolveExpressionAsShort(String value, String attribute, boolean required) {
		Object resolved = resolveExpression(value);
		Short result = null;
		if (resolved instanceof String) {
			if (!required && !StringUtils.hasText((String) resolved)) {
				result = null;
			}
			else {
				result = Short.parseShort((String) resolved);
			}
		}
		else if (resolved instanceof Number) {
			result = ((Number) resolved).shortValue();
		}
		else if (resolved != null || required) {
			throw new IllegalStateException(
					THE_OSQ + attribute + "] must resolve to an Number or a String that can be parsed as a Short. "
							+ RESOLVED_TO_OSQ + (resolved == null ? NULL : resolved.getClass())
									+ CSQ_FOR_OSQ + value + CSQ);
		}
		return result;
	}

	private Long resolveExpressionAsLong(String value, String attribute, boolean required) {
		Object resolved = resolveExpression(value);
		Long result = null;
		if (resolved instanceof String) {
			if (!required && !StringUtils.hasText((String) resolved)) {
				result = null;
			}
			else {
				result = Long.parseLong((String) resolved);
			}
		}
		else if (resolved instanceof Number) {
			result = ((Number) resolved).longValue();
		}
		else if (resolved != null || required) {
			throw new IllegalStateException(
					THE_OSQ + attribute + "] must resolve to an Number or a String that can be parsed as a Long. "
							+ RESOLVED_TO_OSQ + (resolved == null ? NULL : resolved.getClass())
									+ CSQ_FOR_OSQ + value + CSQ);
		}
		return result;
	}

	private Double resolveExpressionAsDouble(String value, String attribute, boolean required) {
		Object resolved = resolveExpression(value);
		Double result = null;
		if (resolved instanceof String) {
			if (!required && !StringUtils.hasText((String) resolved)) {
				result = null;
			}
			else {
				result = Double.parseDouble((String) resolved);
			}
		}
		else if (resolved instanceof Number) {
			result = ((Number) resolved).doubleValue();
		}
		else if (resolved != null || required) {
			throw new IllegalStateException(
					THE_OSQ + attribute + "] must resolve to an Number or a String that can be parsed as a Double. "
							+ RESOLVED_TO_OSQ + (resolved == null ? NULL : resolved.getClass())
									+ CSQ_FOR_OSQ + value + CSQ);
		}
		return result;
	}

	private Boolean resolveExpressionAsBoolean(String value, String attribute) {
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
					THE_OSQ + attribute + "] must resolve to a Boolean or a String that can be parsed as a Boolean. "
							+ RESOLVED_TO_OSQ + resolved.getClass() + CSQ_FOR_OSQ + value + CSQ);
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	private List<Class<? extends Throwable>> resolveClasses(Class<? extends Throwable>[] fromAnnot, String[] names,
			String type) {

		List<Class<? extends Throwable>> classes = new ArrayList<>(Arrays.asList(fromAnnot));
		try {
			for (String name : names) {
				Class<?> clazz = ClassUtils.forName(name, ClassUtils.getDefaultClassLoader());
				if (!Throwable.class.isAssignableFrom(clazz)) {
					throw new IllegalStateException(type + " entry must be of type Throwable: " + clazz);
				}
				classes.add((Class<? extends Throwable>) clazz);
			}
		}
		catch (ClassNotFoundException | LinkageError ex) {
			throw new IllegalStateException(ex);
		}
		return classes;
	}

	private Object resolveExpression(String value) {
		String resolved = resolve(value);
		if (this.expressionContext != null) {
			return this.resolver.evaluate(resolved, this.expressionContext);
		}
		else {
			return value;
		}
	}

	private String resolve(String value) {
		if (this.beanFactory != null && this.beanFactory instanceof ConfigurableBeanFactory) {
			return ((ConfigurableBeanFactory) this.beanFactory).resolveEmbeddedValue(value);
		}
		return value;
	}

}
