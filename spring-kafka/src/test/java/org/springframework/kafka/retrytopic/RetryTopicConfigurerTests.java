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
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.config.MultiMethodKafkaListenerEndpoint;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * @author Tomaz Fernandes
 * @since 2.7
 */
@ExtendWith(MockitoExtension.class)
class RetryTopicConfigurerTests {

	@Mock
	private DestinationTopicProcessor destinationTopicProcessor;

	@Mock
	private ListenerContainerFactoryResolver containerFactoryResolver;

	@Mock
	private ListenerContainerFactoryConfigurer listenerContainerFactoryConfigurer;

	@Mock
	private BeanFactory beanFactory;

	@Mock
	private DefaultListableBeanFactory defaultListableBeanFactory;

	@Mock
	private RetryTopicConfigurer.EndpointProcessor endpointProcessor;

	@Mock
	private MethodKafkaListenerEndpoint<?, ?> mainEndpoint;

	@Mock
	private MultiMethodKafkaListenerEndpoint<?, ?> multiMethodEndpoint;

	@Mock
	private RetryTopicConfiguration configuration;

	@Mock
	private DestinationTopic.Properties mainDestinationProperties;

	@Mock
	private DestinationTopic.Properties firstRetryDestinationProperties;

	@Mock
	private DestinationTopic.Properties secondRetryDestinationProperties;

	@Mock
	private DestinationTopic.Properties dltDestinationProperties;

	@Mock
	private ListenerContainerFactoryResolver.Configuration factoryResolverConfig;

	@Mock
	private ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory;

	@Mock
	private RetryTopicConfiguration.TopicCreation topicCreationConfig;

	@Mock
	private EndpointHandlerMethod endpointHandlerMethod;

	@Mock
	private ConsumerRecord<?, ?> consumerRecordMessage;

	@Mock
	private ListenerContainerFactoryConfigurer.Configuration lcfcConfiguration;

	private static final Object objectMessage = new Object();

	private static final List<String> topics = Arrays.asList("topic1", "topic2");

	private static final String defaultFactoryBeanName = "defaultTestFactory";

	// Captors

	@Captor
	private ArgumentCaptor<Consumer<DestinationTopic.Properties>> destinationPropertiesProcessorCaptor;

	@Captor
	private ArgumentCaptor<DestinationTopicProcessor.Context> contextCaptor;

	@Captor
	private ArgumentCaptor<String> mainTopicNameCaptor;

	@Captor
	private ArgumentCaptor<String> retryDltTopicNameCaptor;

	@Captor
	private ArgumentCaptor<MethodKafkaListenerEndpoint<?, ?>> endpointCaptor;

	@Captor
	private ArgumentCaptor<Consumer<Collection<String>>> topicsConsumerCaptor;

	// Methods

	private final String noOpsMethodName = "noOpsMethod";

	private final String noOpsDltMethodName = "noOpsDltMethod";

	private final Method endpointMethod = getMethod(noOpsMethodName);

	private final Method noOpsDltMethod = getMethod(noOpsDltMethodName);

	private final Object bean = new Object();

	@Mock
	private KafkaListenerEndpointRegistrar registrar;

	private Method getMethod(String methodName)  {
		try {
			return this.getClass().getMethod(methodName);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	void shouldThrowIfMultiMethodEndpoint() {

		// setup
		RetryTopicConfigurer configurer = new RetryTopicConfigurer(destinationTopicProcessor, containerFactoryResolver,
				listenerContainerFactoryConfigurer, beanFactory, new SuffixingRetryTopicNamesProviderFactory());

		// when - then
		assertThatIllegalArgumentException().isThrownBy(
				() -> configurer.processMainAndRetryListeners(endpointProcessor, multiMethodEndpoint, configuration,
						registrar, containerFactory, defaultFactoryBeanName));
	}

	@Test
	void shouldConfigureRetryEndpoints() {

		// given

		List<DestinationTopic.Properties> destinationPropertiesList =
				Arrays.asList(mainDestinationProperties, firstRetryDestinationProperties,
						secondRetryDestinationProperties, dltDestinationProperties);

		RetryTopicConfigurer.EndpointProcessor endpointProcessor = endpoint -> {
			endpoint.setTopics(topics.toArray(new String[]{}));
			endpoint.setId("testId");
			endpoint.setGroup("testGroup");
			endpoint.setGroupId("testGroupId");
			endpoint.setClientIdPrefix("testClientPrefix");
		};
		String mainEndpointSuffix = "";
		String firstRetrySuffix = "-retry-1000";
		String secondRetrySuffix = "-retry-2000";
		String dltSuffix = "-dlt";

		given(configuration.getDestinationTopicProperties()).willReturn(destinationPropertiesList);
		given(mainEndpoint.getBean()).willReturn(bean);
		given(mainEndpoint.getMethod()).willReturn(endpointMethod);
		given(configuration.getDltHandlerMethod()).willReturn(endpointHandlerMethod);
		given(configuration.forKafkaTopicAutoCreation()).willReturn(topicCreationConfig);
		given(topicCreationConfig.shouldCreateTopics()).willReturn(true);

		given(configuration.forContainerFactoryResolver()).willReturn(factoryResolverConfig);
		willReturn(containerFactory).given(containerFactoryResolver)
				.resolveFactoryForMainEndpoint(any(KafkaListenerContainerFactory.class),
				eq(defaultFactoryBeanName), eq(factoryResolverConfig));
		given(mainDestinationProperties.suffix()).willReturn(mainEndpointSuffix);
		given(firstRetryDestinationProperties.suffix()).willReturn(firstRetrySuffix);
		given(secondRetryDestinationProperties.suffix()).willReturn(secondRetrySuffix);
		given(dltDestinationProperties.suffix()).willReturn(dltSuffix);
		given(mainDestinationProperties.isMainEndpoint()).willReturn(true);
		given(mainEndpoint.getTopics()).willReturn(topics);
		given(configuration.forContainerFactoryConfigurer()).willReturn(lcfcConfiguration);

		willReturn(containerFactory).given(containerFactoryResolver).resolveFactoryForRetryEndpoint(containerFactory,
				defaultFactoryBeanName, factoryResolverConfig);
		willReturn(containerFactory).given(this.listenerContainerFactoryConfigurer).configure(containerFactory,
				lcfcConfiguration);
		willReturn(containerFactory).given(this.listenerContainerFactoryConfigurer).configureWithoutBackOffValues(containerFactory,
				lcfcConfiguration);

		RetryTopicConfigurer configurer = new RetryTopicConfigurer(destinationTopicProcessor, containerFactoryResolver,
				listenerContainerFactoryConfigurer, defaultListableBeanFactory, new SuffixingRetryTopicNamesProviderFactory());

		// when
		configurer.processMainAndRetryListeners(endpointProcessor, mainEndpoint, configuration, registrar,
				containerFactory, defaultFactoryBeanName);

		// then

		then(destinationTopicProcessor).should(times(1))
				.processDestinationTopicProperties(destinationPropertiesProcessorCaptor.capture(), contextCaptor.capture());
		DestinationTopicProcessor.Context context = contextCaptor.getValue();
		Consumer<DestinationTopic.Properties> destinationPropertiesConsumer = destinationPropertiesProcessorCaptor.getValue();

		destinationPropertiesConsumer.accept(mainDestinationProperties);
		assertTopicNames(mainDestinationProperties.suffix(), mainDestinationProperties, context, 0);

		destinationPropertiesConsumer.accept(firstRetryDestinationProperties);
		assertTopicNames(firstRetrySuffix, firstRetryDestinationProperties, context, 2);

		destinationPropertiesConsumer.accept(secondRetryDestinationProperties);
		assertTopicNames(secondRetrySuffix, secondRetryDestinationProperties, context, 4);

		destinationPropertiesConsumer.accept(dltDestinationProperties);
		assertTopicNames(dltSuffix, dltDestinationProperties, context, 6);

		then(registrar).should(times(4)).registerEndpoint(endpointCaptor.capture(), eq(this.containerFactory));
		List<MethodKafkaListenerEndpoint<?, ?>> allRegisteredEndpoints = endpointCaptor.getAllValues();

		assertThat(allRegisteredEndpoints.get(0)).isEqualTo(mainEndpoint);

		List<String> firstRetryTopics = new ArrayList<>(allRegisteredEndpoints.get(1).getTopics());
		List<String> secondRetryTopics = new ArrayList<>(allRegisteredEndpoints.get(2).getTopics());
		List<String> dltTopics = new ArrayList<>(allRegisteredEndpoints.get(3).getTopics());

		assertThat(firstRetryTopics.get(0)).isEqualTo(topics.get(0) + firstRetrySuffix);
		assertThat(firstRetryTopics.get(1)).isEqualTo(topics.get(1) + firstRetrySuffix);
		assertThat(secondRetryTopics.get(0)).isEqualTo(topics.get(0) + secondRetrySuffix);
		assertThat(secondRetryTopics.get(1)).isEqualTo(topics.get(1) + secondRetrySuffix);
		assertThat(dltTopics.get(0)).isEqualTo(topics.get(0) + dltSuffix);
		assertThat(dltTopics.get(1)).isEqualTo(topics.get(1) + dltSuffix);

		assertThat(ReflectionTestUtils.getField(allRegisteredEndpoints.get(1), "beanFactory")).isEqualTo(this.defaultListableBeanFactory);
		assertThat(ReflectionTestUtils.getField(allRegisteredEndpoints.get(2), "beanFactory")).isEqualTo(this.defaultListableBeanFactory);
		assertThat(ReflectionTestUtils.getField(allRegisteredEndpoints.get(3), "beanFactory")).isEqualTo(this.defaultListableBeanFactory);

		then(destinationTopicProcessor).should(times(1)).processRegisteredDestinations(topicsConsumerCaptor.capture(), eq(context));

		Consumer<Collection<String>> topicsConsumer = topicsConsumerCaptor.getValue();
		topicsConsumer.accept(topics);

		then(defaultListableBeanFactory).should(times(2))
				.registerSingleton(any(String.class), any(NewTopic.class));
	}

	private void assertTopicNames(String retrySuffix, DestinationTopic.Properties destinationProperties, DestinationTopicProcessor.Context context, int index) {
		then(destinationTopicProcessor).should(times(2)).registerDestinationTopic(mainTopicNameCaptor.capture(),
				retryDltTopicNameCaptor.capture(), eq(destinationProperties), eq(context));

		String firstTopicName = topics.get(0) + retrySuffix;
		String secondTopicName = topics.get(1) + retrySuffix;

		List<String> allValues = mainTopicNameCaptor.getAllValues();
		List<String> retryTopicName = retryDltTopicNameCaptor.getAllValues();
		assertThat(allValues.get(index)).isEqualTo(topics.get(0));
		assertThat(allValues.get(index + 1)).isEqualTo(topics.get(1));
		assertThat(retryTopicName.get(index)).isEqualTo(firstTopicName);
		assertThat(retryTopicName.get(index + 1)).isEqualTo(secondTopicName);
	}

	private void thenAssertEndpointProcessing(MethodKafkaListenerEndpoint<?, ?> endpoint) {
		then(endpoint).should(times(1)).setTopics(topics.toArray(new String[]{}));
		then(endpoint).should(times(1)).setId("testId");
		then(endpoint).should(times(1)).setGroup("testGroup");
		then(endpoint).should(times(1)).setGroupId("testGroupId");
		then(endpoint).should(times(1)).setClientIdPrefix("testClientPrefix");
		then(endpoint).should(times(1)).setBeanFactory(defaultListableBeanFactory);
	}

	public void noOpsMethod() {
		// noOps
	}

	public void noOpsDltMethod() {
		// noOps
	}


	// EndpointHandlerMethod tests

	@Test
	void shouldGetBeanFromContainer() {

		// setup
		NoOpsClass noOps = new NoOpsClass();
		willReturn(noOps).given(beanFactory).getBean(NoOpsClass.class);
		EndpointHandlerMethod handlerMethod =
				RetryTopicConfigurer.createHandlerMethodWith(NoOpsClass.class, noOpsMethodName);

		// given
		Object resolvedBean = handlerMethod.resolveBean(this.beanFactory);

		// then
		assertThat(resolvedBean).isEqualTo(noOps);

	}

	@Test
	void shouldInstantiateIfNotInContainer() {

		// setup
		String beanName = NoOpsClass.class.getSimpleName() + "-handlerMethod";
		given(defaultListableBeanFactory.getBean(beanName)).willReturn(new NoOpsClass());
		willThrow(NoSuchBeanDefinitionException.class).given(defaultListableBeanFactory).getBean(NoOpsClass.class);
		EndpointHandlerMethod handlerMethod =
				RetryTopicConfigurer.createHandlerMethodWith(NoOpsClass.class, noOpsMethodName);

		// given
		Object resolvedBean = handlerMethod.resolveBean(this.defaultListableBeanFactory);

		// then
		then(defaultListableBeanFactory).should()
				.registerBeanDefinition(eq(beanName), any(RootBeanDefinition.class));
		assertThat(NoOpsClass.class.isAssignableFrom(resolvedBean.getClass())).isTrue();

	}

	@Test
	void shouldLogConsumerRecordMessage() {
		RetryTopicConfigurer.LoggingDltListenerHandlerMethod method =
				new RetryTopicConfigurer.LoggingDltListenerHandlerMethod();
		method.logMessage(consumerRecordMessage);
		then(consumerRecordMessage).should(never()).topic();
	}

	@Test
	void shouldNotLogObjectMessage() {
		RetryTopicConfigurer.LoggingDltListenerHandlerMethod method =
				new RetryTopicConfigurer.LoggingDltListenerHandlerMethod();
		method.logMessage(objectMessage);
	}

	static class NoOpsClass {
		void noOpsMethod() { };
	}
}
