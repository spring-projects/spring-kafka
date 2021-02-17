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
import java.util.Map;

import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurer;


/**
 *
 * Attempts to provide an instance of {@link RetryTopicConfigurer} by either creating one
 * from a {@link RetryableTopic} annotation, or from the bean container if no annotation
 * is available.
 *
 * If beans are found in the container there's a check to determine whether or not
 * the provided topics topics should be handled by any of such instances.
 *
 * If the annotation is provided, a {@link org.springframework.kafka.annotation.DltHandler}
 * annotated method is looked up.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 * @see RetryTopicConfigurer
 * @see RetryableTopic
 * @see org.springframework.kafka.annotation.DltHandler
 *
 */
public class RetryTopicConfigurationProvider {

	private final BeanFactory beanFactory;

	private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(RetryTopicConfigurationProvider.class));

	public RetryTopicConfigurationProvider(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}
	public RetryTopicConfiguration findRetryConfigurationFor(String[] topics, Method method, Object bean) {
		RetryableTopic annotation = AnnotationUtils.findAnnotation(method, RetryableTopic.class);
		return annotation != null
				? new RetryableTopicAnnotationProcessor(this.beanFactory)
				.processAnnotation(topics, method, annotation, bean)
				: maybeGetFromContext(topics);
	}

	private RetryTopicConfiguration maybeGetFromContext(String[] topics) {
		if (this.beanFactory == null || !ListableBeanFactory.class.isAssignableFrom(this.beanFactory.getClass())) {
			LOGGER.warn("No ListableBeanFactory found, skipping RetryTopic configuration.");
			return null;
		}

		Map<String, RetryTopicConfiguration> retryTopicProcessors = ((ListableBeanFactory) this.beanFactory)
				.getBeansOfType(RetryTopicConfiguration.class);
		return retryTopicProcessors
				.values()
				.stream()
				.filter(topicConfiguration -> topicConfiguration.hasConfigurationForTopics(topics))
				.findFirst()
				.orElse(null);
	}
}
