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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.BDDMockito.given;

import java.lang.reflect.Method;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.annotation.RetryableTopicAnnotationProcessor;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.retry.annotation.Backoff;
import org.springframework.util.ReflectionUtils;

/**
 * @author Tomaz Fernandes
 * @since 2.7
 */
@ExtendWith(MockitoExtension.class)
class RetryableTopicAnnotationProcessorTests {

	private final String topic1 = "topic1";

	private final String topic2 = "topic2";

	private final String[] topics = {topic1, topic2};

	private static final String kafkaTemplateName = "kafkaTemplateBean";

	private final String listenerMethodName = "listenWithRetry";

	@Mock
	private KafkaOperations<?, ?> kafkaOperationsFromTemplateName;

	@Mock
	private KafkaOperations<?, ?> kafkaOperationsFromDefaultName;

	@Mock
	private BeanFactory beanFactory;

	// Retry with DLT
	private Method listenWithRetryAndDlt = ReflectionUtils.findMethod(RetryableTopicAnnotationFactoryWithDlt.class, listenerMethodName);

	private RetryableTopic annotationWithDlt = AnnotationUtils.findAnnotation(listenWithRetryAndDlt, RetryableTopic.class);

	private Object beanWithDlt = createBean();

	// Retry without DLT
	private Method listenWithRetry = ReflectionUtils.findMethod(RetryableTopicAnnotationFactory.class, listenerMethodName);

	private RetryableTopic annotation = AnnotationUtils.findAnnotation(listenWithRetry, RetryableTopic.class);

	private Object bean = createBean();

	private Object createBean() {
		try {
			return RetryableTopicAnnotationFactory.class.getDeclaredConstructor().newInstance();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	void shouldGetDltHandlerMethod() {

		// setup
		given(beanFactory.getBean(RetryTopicInternalBeanNames.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class))
				.willReturn(kafkaOperationsFromDefaultName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given
		RetryTopicConfiguration configuration = processor
				.processAnnotation(topics, listenWithRetryAndDlt, annotationWithDlt, beanWithDlt);

		// then
		RetryTopicConfigurer.EndpointHandlerMethod dltHandlerMethod = configuration.getDltHandlerMethod();
		assertEquals("handleDlt", dltHandlerMethod.getMethod().getName());

		assertFalse(new DestinationTopic("",
				configuration.getDestinationTopicProperties().get(0)).isAlwaysRetryOnDltFailure());
	}

	@Test
	void shouldGetLoggingDltHandlerMethod() {

		// setup
		given(beanFactory.getBean(kafkaTemplateName, KafkaOperations.class)).willReturn(kafkaOperationsFromTemplateName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given
		RetryTopicConfiguration configuration = processor.processAnnotation(topics, listenWithRetry, annotation, bean);

		// then
		RetryTopicConfigurer.EndpointHandlerMethod dltHandlerMethod = configuration.getDltHandlerMethod();
		assertEquals(RetryTopicConfigurer.LoggingDltListenerHandlerMethod.DEFAULT_DLT_METHOD_NAME,
				dltHandlerMethod.getMethod().getName());

		assertTrue(new DestinationTopic("",
				configuration.getDestinationTopicProperties().get(0)).isAlwaysRetryOnDltFailure());
	}

	@Test
	void shouldThrowIfProvidedKafkaTemplateNotFound() {

		// setup
		given(this.beanFactory.getBean(kafkaTemplateName, KafkaOperations.class)).willThrow(NoSuchBeanDefinitionException.class);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given - then
		assertThrows(BeanInitializationException.class, () ->
				processor.processAnnotation(topics, listenWithRetry, annotation, bean));
	}

	@Test
	void shouldThrowIfNoKafkaTemplateFound() {

		// setup
		given(this.beanFactory.getBean(RetryTopicInternalBeanNames.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class))
				.willThrow(NoSuchBeanDefinitionException.class);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given - then
		assertThrows(BeanInitializationException.class, () ->
				processor.processAnnotation(topics, listenWithRetryAndDlt, annotationWithDlt, beanWithDlt));
	}

	@Test
	void shouldGetKafkaTemplateFromBeanName() {

		// setup
		given(this.beanFactory.getBean(kafkaTemplateName, KafkaOperations.class))
				.willReturn(kafkaOperationsFromTemplateName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given - then
		RetryTopicConfiguration configuration = processor.processAnnotation(topics, listenWithRetry, annotation, bean);
		DestinationTopic.Properties properties = configuration.getDestinationTopicProperties().get(0);
		DestinationTopic destinationTopic = new DestinationTopic("", properties);
		assertEquals(kafkaOperationsFromTemplateName, destinationTopic.getKafkaOperations());
	}

	@Test
	void shouldGetKafkaTemplateFromDefaultBeanName() {

		// setup
		given(this.beanFactory.getBean(RetryTopicInternalBeanNames.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class))
				.willReturn(kafkaOperationsFromDefaultName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given
		RetryTopicConfiguration configuration = processor
				.processAnnotation(topics, listenWithRetryAndDlt, annotationWithDlt, beanWithDlt);

		// then
		DestinationTopic.Properties properties = configuration.getDestinationTopicProperties().get(0);
		DestinationTopic destinationTopic = new DestinationTopic("", properties);
		assertEquals(kafkaOperationsFromDefaultName, destinationTopic.getKafkaOperations());
	}

	@Test
	void shouldCreateExponentialBackoff() {

		// setup
		given(this.beanFactory.getBean(RetryTopicInternalBeanNames.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class))
				.willReturn(kafkaOperationsFromDefaultName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given
		RetryTopicConfiguration configuration = processor
				.processAnnotation(topics, listenWithRetryAndDlt, annotationWithDlt, beanWithDlt);

		// then
		List<DestinationTopic.Properties> destinationTopicProperties = configuration.getDestinationTopicProperties();
		DestinationTopic destinationTopic = new DestinationTopic("", destinationTopicProperties.get(0));
		assertEquals(0, destinationTopic.getDestinationDelay());
		DestinationTopic destinationTopic2 = new DestinationTopic("", destinationTopicProperties.get(1));
		assertEquals(1000, destinationTopic2.getDestinationDelay());
		DestinationTopic destinationTopic3 = new DestinationTopic("", destinationTopicProperties.get(2));
		assertEquals(2000, destinationTopic3.getDestinationDelay());
		DestinationTopic destinationTopic4 = new DestinationTopic("", destinationTopicProperties.get(3));
		assertEquals(0, destinationTopic4.getDestinationDelay());

	}

	@Test
	void shouldSetAbort() {

		// setup
		given(this.beanFactory.getBean(RetryTopicInternalBeanNames.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class))
				.willReturn(kafkaOperationsFromDefaultName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given
		RetryTopicConfiguration configuration = processor
				.processAnnotation(topics, listenWithRetryAndDlt, annotationWithDlt, beanWithDlt);

		// then
		List<DestinationTopic.Properties> destinationTopicProperties = configuration.getDestinationTopicProperties();
		DestinationTopic destinationTopic = new DestinationTopic("", destinationTopicProperties.get(0));
		assertEquals(0, destinationTopic.getDestinationDelay());
		DestinationTopic destinationTopic2 = new DestinationTopic("", destinationTopicProperties.get(1));
		assertEquals(1000, destinationTopic2.getDestinationDelay());
		DestinationTopic destinationTopic3 = new DestinationTopic("", destinationTopicProperties.get(2));
		assertEquals(2000, destinationTopic3.getDestinationDelay());
		DestinationTopic destinationTopic4 = new DestinationTopic("", destinationTopicProperties.get(3));
		assertEquals(0, destinationTopic4.getDestinationDelay());

	}

	@Test
	void shouldCreateFixedBackoff() {

		// setup
		given(this.beanFactory.getBean(kafkaTemplateName, KafkaOperations.class))
				.willReturn(kafkaOperationsFromTemplateName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given
		RetryTopicConfiguration configuration = processor
				.processAnnotation(topics, listenWithRetry, annotation, bean);

		// then
		List<DestinationTopic.Properties> destinationTopicProperties = configuration.getDestinationTopicProperties();
		DestinationTopic destinationTopic = new DestinationTopic("", destinationTopicProperties.get(0));
		assertEquals(0, destinationTopic.getDestinationDelay());
		DestinationTopic destinationTopic2 = new DestinationTopic("", destinationTopicProperties.get(1));
		assertEquals(1000, destinationTopic2.getDestinationDelay());
		DestinationTopic destinationTopic3 = new DestinationTopic("", destinationTopicProperties.get(2));
		assertEquals(1000, destinationTopic3.getDestinationDelay());
		DestinationTopic destinationTopic4 = new DestinationTopic("", destinationTopicProperties.get(3));
		assertEquals(0, destinationTopic4.getDestinationDelay());

	}

	static class RetryableTopicAnnotationFactory {

		@KafkaListener
		@RetryableTopic(kafkaTemplate = RetryableTopicAnnotationProcessorTests.kafkaTemplateName)
		void listenWithRetry() {
			// NoOps
		}
	}

	static class RetryableTopicAnnotationFactoryWithDlt {

		@KafkaListener
		@RetryableTopic(attempts = 3, backoff = @Backoff(multiplier = 2, value = 1000),
			dltStrategy = DltStrategy.FAIL_ON_ERROR)
		void listenWithRetry() {
			// NoOps
		}

		@DltHandler
		void handleDlt() {
			// NoOps
		}
	}
}
