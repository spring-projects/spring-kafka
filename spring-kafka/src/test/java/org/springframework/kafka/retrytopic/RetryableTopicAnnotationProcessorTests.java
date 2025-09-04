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
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.annotation.RetryableTopicAnnotationProcessor;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.support.EndpointHandlerMethod;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.ReflectionUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

/**
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @author Adrian Chlebosz
 * @author Wang Zhiyang
 *
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

	private ConfigurableBeanFactory beanFactory;

	{
		this.beanFactory = mock(ConfigurableBeanFactory.class);
		willAnswer(invoc -> invoc.getArgument(0)).given(this.beanFactory).resolveEmbeddedValue(anyString());
	}

	// Retry with DLT
	private final Method listenWithRetryAndDlt = ReflectionUtils
			.findMethod(RetryableTopicAnnotationFactoryWithDlt.class, listenerMethodName);

	private final RetryableTopic annotationWithDlt = AnnotationUtils.findAnnotation(listenWithRetryAndDlt,
			RetryableTopic.class);

	private final Object beanWithDlt = createBean();

	// Retry without DLT
	private final Method listenWithRetry = ReflectionUtils.findMethod(RetryableTopicAnnotationFactory.class,
			listenerMethodName);

	private final RetryableTopic annotation = AnnotationUtils.findAnnotation(listenWithRetry, RetryableTopic.class);

	private final Object bean = createBean();

	// Retry with custom DLT routing
	private final Method listenWithCustomDltRouting = ReflectionUtils
		.findMethod(RetryableTopicAnnotationFactoryWithCustomDltRouting.class, listenerMethodName);

	private final RetryableTopic annotationWithCustomDltRouting = AnnotationUtils.findAnnotation(
		listenWithCustomDltRouting, RetryableTopic.class);

	private final Object beanWithCustomDltRouting = createBean();

	private Object createBean() {
		try {
			return RetryableTopicAnnotationFactory.class.getDeclaredConstructor().newInstance();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static Stream<Arguments> paramsForRetryTopic() {
		return Stream.of(
				Arguments.of(true),
				Arguments.of(false));
	}

	@ParameterizedTest(name = "{index} shouldGetDltHandlerMethod is method {0}")
	@MethodSource("paramsForRetryTopic")
	void shouldGetDltHandlerMethod(boolean isMethod) {

		// setup
		given(beanFactory.getBean(RetryTopicBeanNames.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class))
			.willReturn(kafkaOperationsFromDefaultName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given
		RetryTopicConfiguration configuration = getRetryTopicConfiguration(processor, topics, listenWithRetryAndDlt,
				RetryableTopicClassLevelAnnotationFactoryWithDlt.class, isMethod, annotationWithDlt, beanWithDlt);

		// then
		EndpointHandlerMethod dltHandlerMethod = configuration.getDltHandlerMethod();
		Method method = (Method) ReflectionTestUtils.getField(dltHandlerMethod, "method");
		assertThat(method.getName()).isEqualTo("handleDlt");

		assertThat(new DestinationTopic("",
			configuration.getDestinationTopicProperties().get(0)).isAlwaysRetryOnDltFailure()).isFalse();
	}

	private RetryTopicConfiguration getRetryTopicConfiguration(RetryableTopicAnnotationProcessor processor,
			String[] topics, Method method, Class<?> clazz, boolean isMethod, RetryableTopic annotation, Object bean) {
		if (isMethod) {
			return processor.processAnnotation(topics, method, annotation, bean);
		}
		else {
			return processor.processAnnotation(topics, clazz, annotation, bean);
		}
	}

	@ParameterizedTest(name = "{index} shouldGetLoggingDltHandlerMethod is method {0}")
	@MethodSource("paramsForRetryTopic")
	void shouldGetLoggingDltHandlerMethod(boolean isMethod) {

		// setup
		given(beanFactory.getBean(kafkaTemplateName, KafkaOperations.class)).willReturn(kafkaOperationsFromTemplateName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given
		RetryTopicConfiguration configuration = getRetryTopicConfiguration(processor, topics, listenWithRetry,
				RetryableTopicClassLevelAnnotationFactory.class, isMethod, annotation, bean);

		// then
		EndpointHandlerMethod dltHandlerMethod = configuration.getDltHandlerMethod();
		dltHandlerMethod.resolveBean(this.beanFactory);
		Method method = dltHandlerMethod.getMethod();
		assertThat(method.getName())
				.isEqualTo(RetryTopicConfigurer.LoggingDltListenerHandlerMethod.DEFAULT_DLT_METHOD_NAME);

		assertThat(new DestinationTopic("",
				configuration.getDestinationTopicProperties().get(0)).isAlwaysRetryOnDltFailure()).isTrue();
	}

	@Test
	void shouldThrowIfProvidedKafkaTemplateNotFound() {

		// setup
		given(this.beanFactory.getBean(kafkaTemplateName, KafkaOperations.class)).willThrow(NoSuchBeanDefinitionException.class);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given - then
		assertThatExceptionOfType(BeanInitializationException.class)
				.isThrownBy(() -> processor.processAnnotation(topics, listenWithRetry, annotation, bean));
		assertThatExceptionOfType(BeanInitializationException.class)
				.isThrownBy(() -> processor.processAnnotation(topics, RetryableTopicClassLevelAnnotationFactory.class,
						annotation, bean));
	}

	@Test
	void shouldThrowIfNoKafkaTemplateFound() {

		// setup
		given(this.beanFactory.getBean(RetryTopicBeanNames.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class))
				.willThrow(NoSuchBeanDefinitionException.class);

		@SuppressWarnings({ "unchecked", "rawtypes" })
		ObjectProvider<KafkaOperations> templateProvider = mock(ObjectProvider.class);
		given(templateProvider.getIfUnique()).willReturn(null);
		given(this.beanFactory.getBeanProvider(KafkaOperations.class))
				.willReturn(templateProvider);

		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given - then
		assertThatIllegalStateException().isThrownBy(() ->
				processor.processAnnotation(topics, listenWithRetryAndDlt, annotationWithDlt, beanWithDlt));
		assertThatIllegalStateException().isThrownBy(() -> processor.processAnnotation(
				topics, RetryableTopicClassLevelAnnotationFactoryWithDlt.class, annotationWithDlt, beanWithDlt));
	}

	@ParameterizedTest(name = "{index} shouldTrySpringBootDefaultKafkaTemplate is method {0}")
	@MethodSource("paramsForRetryTopic")
	void shouldTrySpringBootDefaultKafkaTemplate(boolean isMethod) {

		// setup
		given(this.beanFactory.getBean(RetryTopicBeanNames.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class))
				.willThrow(NoSuchBeanDefinitionException.class);
		@SuppressWarnings({ "unchecked", "rawtypes" })
		ObjectProvider<KafkaOperations> templateProvider = mock(ObjectProvider.class);
		given(templateProvider.getIfUnique()).willReturn(kafkaOperationsFromDefaultName);
		given(this.beanFactory.getBeanProvider(KafkaOperations.class))
				.willReturn(templateProvider);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given - then
		RetryTopicConfiguration configuration = getRetryTopicConfiguration(processor, topics, listenWithRetry,
				RetryableTopicClassLevelAnnotationFactory.class, isMethod, annotationWithDlt, bean);
		DestinationTopic.Properties properties = configuration.getDestinationTopicProperties().get(0);
		DestinationTopic destinationTopic = new DestinationTopic("", properties);
		assertThat(destinationTopic.getKafkaOperations()).isEqualTo(kafkaOperationsFromDefaultName);
	}

	@ParameterizedTest(name = "{index} shouldGetKafkaTemplateFromBeanName is method {0}")
	@MethodSource("paramsForRetryTopic")
	void shouldGetKafkaTemplateFromBeanName(boolean isMethod) {

		// setup
		given(this.beanFactory.getBean(kafkaTemplateName, KafkaOperations.class))
				.willReturn(kafkaOperationsFromTemplateName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given - then
		RetryTopicConfiguration configuration = getRetryTopicConfiguration(processor, topics, listenWithRetry,
				RetryableTopicClassLevelAnnotationFactory.class, isMethod, annotation, bean);
		DestinationTopic.Properties properties = configuration.getDestinationTopicProperties().get(0);
		DestinationTopic destinationTopic = new DestinationTopic("", properties);
		assertThat(destinationTopic.getKafkaOperations()).isEqualTo(kafkaOperationsFromTemplateName);
	}

	@ParameterizedTest(name = "{index} shouldGetKafkaTemplateFromDefaultBeanName is method {0}")
	@MethodSource("paramsForRetryTopic")
	void shouldGetKafkaTemplateFromDefaultBeanName(boolean isMethod) {

		// setup
		given(this.beanFactory.getBean(RetryTopicBeanNames.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class))
				.willReturn(kafkaOperationsFromDefaultName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given
		RetryTopicConfiguration configuration = getRetryTopicConfiguration(processor, topics, listenWithRetryAndDlt,
				RetryableTopicClassLevelAnnotationFactoryWithDlt.class, isMethod, annotationWithDlt, beanWithDlt);

		// then
		DestinationTopic.Properties properties = configuration.getDestinationTopicProperties().get(0);
		DestinationTopic destinationTopic = new DestinationTopic("", properties);
		assertThat(destinationTopic.getKafkaOperations()).isEqualTo(kafkaOperationsFromDefaultName);
	}

	@ParameterizedTest(name = "{index} shouldCreateExponentialBackoff is method {0}")
	@MethodSource("paramsForRetryTopic")
	void shouldCreateExponentialBackOff(boolean isMethod) {

		// setup
		given(this.beanFactory.getBean(RetryTopicBeanNames.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class))
				.willReturn(kafkaOperationsFromDefaultName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given
		RetryTopicConfiguration configuration = getRetryTopicConfiguration(processor, topics, listenWithRetryAndDlt,
				RetryableTopicClassLevelAnnotationFactoryWithDlt.class, isMethod, annotationWithDlt, beanWithDlt);

		// then
		List<DestinationTopic.Properties> destinationTopicProperties = configuration.getDestinationTopicProperties();
		DestinationTopic destinationTopic = new DestinationTopic("", destinationTopicProperties.get(0));
		assertThat(destinationTopic.getDestinationDelay()).isEqualTo(0);
		DestinationTopic destinationTopic2 = new DestinationTopic("", destinationTopicProperties.get(1));
		assertThat(destinationTopic2.getDestinationDelay()).isEqualTo(1000);
		DestinationTopic destinationTopic3 = new DestinationTopic("", destinationTopicProperties.get(2));
		assertThat(destinationTopic3.getDestinationDelay()).isEqualTo(2000);
		DestinationTopic destinationTopic4 = new DestinationTopic("", destinationTopicProperties.get(3));
		assertThat(destinationTopic4.getDestinationDelay()).isEqualTo(0);

		assertThat(destinationTopic.shouldRetryOn(1, new IllegalStateException())).isFalse();
		assertThat(destinationTopic.shouldRetryOn(1, new IllegalArgumentException())).isTrue();
	}

	@ParameterizedTest(name = "{index} shouldSetAbort is method {0}")
	@MethodSource("paramsForRetryTopic")
	void shouldSetAbort(boolean isMethod) {

		// setup
		given(this.beanFactory.getBean(RetryTopicBeanNames.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class))
				.willReturn(kafkaOperationsFromDefaultName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given
		RetryTopicConfiguration configuration = getRetryTopicConfiguration(processor, topics, listenWithRetryAndDlt,
				RetryableTopicClassLevelAnnotationFactoryWithDlt.class, isMethod, annotationWithDlt, beanWithDlt);

		// then
		List<DestinationTopic.Properties> destinationTopicProperties = configuration.getDestinationTopicProperties();
		DestinationTopic destinationTopic = new DestinationTopic("", destinationTopicProperties.get(0));
		assertThat(destinationTopic.getDestinationDelay()).isEqualTo(0);
		DestinationTopic destinationTopic2 = new DestinationTopic("", destinationTopicProperties.get(1));
		assertThat(destinationTopic2.getDestinationDelay()).isEqualTo(1000);
		DestinationTopic destinationTopic3 = new DestinationTopic("", destinationTopicProperties.get(2));
		assertThat(destinationTopic3.getDestinationDelay()).isEqualTo(2000);
		DestinationTopic destinationTopic4 = new DestinationTopic("", destinationTopicProperties.get(3));
		assertThat(destinationTopic4.getDestinationDelay()).isEqualTo(0);

	}

	@ParameterizedTest(name = "{index} shouldCreateFixedBackoff is method {0}")
	@MethodSource("paramsForRetryTopic")
	void shouldCreateFixedBackOff(boolean isMethod) {

		// setup
		given(this.beanFactory.getBean(kafkaTemplateName, KafkaOperations.class))
				.willReturn(kafkaOperationsFromTemplateName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given
		RetryTopicConfiguration configuration = getRetryTopicConfiguration(processor, topics, listenWithRetry,
				RetryableTopicClassLevelAnnotationFactory.class, isMethod, annotation, bean);

		// then
		List<DestinationTopic.Properties> destinationTopicProperties = configuration.getDestinationTopicProperties();
		assertThat(destinationTopicProperties.size()).isEqualTo(3);
		DestinationTopic destinationTopic = new DestinationTopic("", destinationTopicProperties.get(0));
		assertThat(destinationTopic.getDestinationDelay()).isEqualTo(0);
		DestinationTopic destinationTopic2 = new DestinationTopic("", destinationTopicProperties.get(1));
		assertThat(destinationTopic2.getDestinationDelay()).isEqualTo(1000);
		DestinationTopic destinationTopic3 = new DestinationTopic("", destinationTopicProperties.get(2));
		assertThat(destinationTopic3.getDestinationDelay()).isEqualTo(0);
	}

	@ParameterizedTest(name = "{index} shouldCreateFixedBackoff is method {0}")
	@MethodSource("paramsForRetryTopic")
	void shouldCreateExceptionBasedDltRoutingSpec(boolean isMethod) {
		// setup
		given(this.beanFactory.getBean(RetryTopicBeanNames.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class))
			.willReturn(kafkaOperationsFromDefaultName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// given
		RetryTopicConfiguration configuration = getRetryTopicConfiguration(processor, topics,
				listenWithCustomDltRouting, RetryableTopicClassLevelAnnotationFactoryWithCustomDltRouting.class,
				isMethod, annotationWithCustomDltRouting, beanWithCustomDltRouting);

		// then
		List<DestinationTopic.Properties> destinationTopicProperties = configuration.getDestinationTopicProperties();

		assertThat(destinationTopicProperties).hasSize(3);
		assertThat(destinationTopicProperties.get(0).suffix()).isEmpty();
		assertThat(destinationTopicProperties.get(1).suffix()).isEqualTo("-deserialization-dlt");
		assertThat(destinationTopicProperties.get(2).suffix()).isEqualTo("-dlt");
	}

	static class RetryableTopicAnnotationFactory {

		@KafkaListener
		@RetryableTopic(kafkaTemplate = RetryableTopicAnnotationProcessorTests.kafkaTemplateName)
		void listenWithRetry() {
			// NoOps
		}
	}

	@KafkaListener
	@RetryableTopic(kafkaTemplate = RetryableTopicAnnotationProcessorTests.kafkaTemplateName)
	static class RetryableTopicClassLevelAnnotationFactory {

	}

	static class RetryableTopicAnnotationFactoryWithDlt {

		@KafkaListener
		@RetryableTopic(attempts = "3", backOff = @BackOff(multiplier = 2, value = 1000),
			dltStrategy = DltStrategy.FAIL_ON_ERROR, excludeNames = "java.lang.IllegalStateException")
		void listenWithRetry() {
			// NoOps
		}

		@DltHandler
		void handleDlt() {
			// NoOps
		}
	}

	@KafkaListener
	@RetryableTopic(attempts = "3", backOff = @BackOff(multiplier = 2, value = 1000),
			dltStrategy = DltStrategy.FAIL_ON_ERROR, excludeNames = "java.lang.IllegalStateException")
	static class RetryableTopicClassLevelAnnotationFactoryWithDlt {

		@DltHandler
		void handleDlt() {
			// NoOps
		}

	}

	static class RetryableTopicAnnotationFactoryWithCustomDltRouting {
		@KafkaListener
		@RetryableTopic(
			attempts = "1",
			exceptionBasedDltRouting = {
				@ExceptionBasedDltDestination(
					suffix = "-deserialization", exceptions = {DeserializationException.class}
				)
			}
		)
		void listenWithRetry() {
			// NoOps
		}
	}

	@KafkaListener
	@RetryableTopic(
			attempts = "1",
			exceptionBasedDltRouting = {
					@ExceptionBasedDltDestination(
							suffix = "-deserialization", exceptions = {DeserializationException.class}
					)
			}
	)
	static class RetryableTopicClassLevelAnnotationFactoryWithCustomDltRouting {
	}

}
