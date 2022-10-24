/*
 * Copyright 2021-2022 the original author or authors.
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerPartitionPausingBackOffManagerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.listener.ListenerContainerRegistry;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.util.backoff.BackOff;

/**
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.9
 */
class RetryTopicConfigurationSupportTests {

	@BeforeEach
	void reset() throws Exception { // NOSONAR
		Field field = RetryTopicConfigurationSupport.class.getDeclaredField("ONLY_ONE_ALLOWED");
		field.setAccessible(true);
		((AtomicBoolean) field.get(null)).set(true);
	}

	@SuppressWarnings("unchecked")
	@Test
	void testCreateConfigurer() {
		RetryTopicComponentFactory componentFactory = mock(RetryTopicComponentFactory.class);
		KafkaConsumerBackoffManager backoffManager = mock(KafkaConsumerBackoffManager.class);
		DestinationTopicResolver resolver = mock(DestinationTopicResolver.class);
		DestinationTopicProcessor processor = mock(DestinationTopicProcessor.class);
		ListenerContainerFactoryConfigurer lcfc = mock(ListenerContainerFactoryConfigurer.class);
		ListenerContainerFactoryResolver lcfr = mock(ListenerContainerFactoryResolver.class);
		RetryTopicNamesProviderFactory namesProviderFactory = mock(RetryTopicNamesProviderFactory.class);
		BeanFactory beanFactory = mock(BeanFactory.class);
		DeadLetterPublishingRecovererFactory dlprf = mock(DeadLetterPublishingRecovererFactory.class);
		RetryTopicConfigurer topicConfigurer = mock(RetryTopicConfigurer.class);
		Clock clock = mock(Clock.class);

		given(componentFactory.deadLetterPublishingRecovererFactory(resolver)).willReturn(dlprf);
		given(componentFactory.listenerContainerFactoryConfigurer(backoffManager, dlprf, clock)).willReturn(lcfc);
		given(componentFactory.listenerContainerFactoryResolver(beanFactory)).willReturn(lcfr);
		given(componentFactory.internalRetryTopicClock()).willReturn(clock);
		given(componentFactory.destinationTopicProcessor(resolver)).willReturn(processor);
		given(componentFactory.retryTopicNamesProviderFactory()).willReturn(namesProviderFactory);
		given(componentFactory.retryTopicConfigurer(processor, lcfc, lcfr, namesProviderFactory)).willReturn(topicConfigurer);

		Consumer<ConcurrentMessageListenerContainer<?, ?>> listenerContainerCustomizer = mock(Consumer.class);
		Consumer<DeadLetterPublishingRecoverer> dlprCustomizer = mock(Consumer.class);
		Consumer<DeadLetterPublishingRecovererFactory> dlprfCustomizer = mock(Consumer.class);
		Consumer<RetryTopicConfigurer> rtconfigurer = mock(Consumer.class);
		Consumer<ListenerContainerFactoryConfigurer> lcfcConsumer = mock(Consumer.class);
		Consumer<DefaultErrorHandler> errorHandlerCustomizer = mock(Consumer.class);
		BackOff backoff = mock(BackOff.class);

		RetryTopicConfigurationSupport support = new RetryTopicConfigurationSupport() {
			@Override
			protected RetryTopicComponentFactory createComponentFactory() {
				return componentFactory;
			}

			@Override
			protected void configureCustomizers(CustomizersConfigurer customizersConfigurer) {
				customizersConfigurer
						.customizeDeadLetterPublishingRecoverer(dlprCustomizer)
						.customizeListenerContainer(listenerContainerCustomizer)
						.customizeErrorHandler(errorHandlerCustomizer);
			}

			@Override
			protected Consumer<ListenerContainerFactoryConfigurer> configureListenerContainerFactoryConfigurer() {
				return lcfcConsumer;
			}

			@Override
			protected Consumer<DeadLetterPublishingRecovererFactory> configureDeadLetterPublishingContainerFactory() {
				return dlprfCustomizer;
			}

			@Override
			protected Consumer<RetryTopicConfigurer> configureRetryTopicConfigurer() {
				return rtconfigurer;
			}

			@Override
			protected void configureBlockingRetries(BlockingRetriesConfigurer blockingRetries) {
				blockingRetries
						.retryOn(RuntimeException.class)
						.backOff(backoff);
			}
		};

		RetryTopicConfigurer retryTopicConfigurer = support.retryTopicConfigurer(backoffManager, resolver, beanFactory);
		assertThat(retryTopicConfigurer).isNotNull();

		then(componentFactory).should().destinationTopicProcessor(resolver);
		then(componentFactory).should().deadLetterPublishingRecovererFactory(resolver);
		then(componentFactory).should().listenerContainerFactoryConfigurer(backoffManager, dlprf, clock);
		then(componentFactory).should().listenerContainerFactoryResolver(beanFactory);
		then(componentFactory).should().retryTopicNamesProviderFactory();
		then(componentFactory).should().retryTopicConfigurer(processor, lcfc, lcfr, namesProviderFactory);

		then(dlprf).should().setDeadLetterPublishingRecovererCustomizer(dlprCustomizer);
		then(lcfc).should().setContainerCustomizer(listenerContainerCustomizer);
		then(lcfc).should().setErrorHandlerCustomizer(errorHandlerCustomizer);
		assertThatThrownBy(lcfc::setBlockingRetryableExceptions).isInstanceOf(IllegalStateException.class);
		then(lcfc).should().setBlockingRetriesBackOff(backoff);
		then(lcfc).should().setRetainStandardFatal(true);
		then(dlprfCustomizer).should().accept(dlprf);
		then(rtconfigurer).should().accept(topicConfigurer);
		then(lcfcConsumer).should().accept(lcfc);

	}

	@Test
	void testRetryTopicConfigurerNoConfiguration() {
		KafkaConsumerBackoffManager backoffManager = mock(KafkaConsumerBackoffManager.class);
		DestinationTopicResolver resolver = mock(DestinationTopicResolver.class);
		BeanFactory beanFactory = mock(BeanFactory.class);
		RetryTopicConfigurationSupport support = new RetryTopicConfigurationSupport();
		RetryTopicConfigurer retryTopicConfigurer = support.retryTopicConfigurer(backoffManager, resolver, beanFactory);
		assertThat(retryTopicConfigurer).isNotNull();
	}

	@Test
	void testCreateBackOffManager() {
		ListenerContainerRegistry registry = mock(ListenerContainerRegistry.class);
		RetryTopicComponentFactory componentFactory = mock(RetryTopicComponentFactory.class);
		ContainerPartitionPausingBackOffManagerFactory factory = mock(
				ContainerPartitionPausingBackOffManagerFactory.class);
		KafkaConsumerBackoffManager backoffManagerMock = mock(KafkaConsumerBackoffManager.class);
		TaskScheduler taskSchedulerMock = mock(TaskScheduler.class);
		Clock clock = mock(Clock.class);
		ApplicationContext ctx = mock(ApplicationContext.class);
		given(componentFactory.kafkaBackOffManagerFactory(registry, ctx)).willReturn(factory);
		given(factory.create()).willReturn(backoffManagerMock);
		RetryTopicConfigurationSupport support = new RetryTopicConfigurationSupport() {

			@Override
			protected RetryTopicComponentFactory createComponentFactory() {
				return componentFactory;
			}

		};
		KafkaConsumerBackoffManager backoffManager = support.kafkaConsumerBackoffManager(ctx, registry, null,
				taskSchedulerMock);
		assertThat(backoffManager).isEqualTo(backoffManagerMock);
		then(componentFactory).should().kafkaBackOffManagerFactory(registry, ctx);
		then(factory).should().create();
	}

	@Test
	void testCreateBackOffManagerNoConfiguration() {
		ListenerContainerRegistry registry = mock(ListenerContainerRegistry.class);
		TaskScheduler scheduler = mock(TaskScheduler.class);
		ApplicationContext ctx = mock(ApplicationContext.class);
		RetryTopicConfigurationSupport support = new RetryTopicConfigurationSupport();
		KafkaConsumerBackoffManager backoffManager = support.kafkaConsumerBackoffManager(ctx, registry, null,
				scheduler);
		assertThat(backoffManager).isNotNull();
	}

	@SuppressWarnings("unchecked")
	@Test
	void testCreateDestinationTopicResolver() {
		RetryTopicComponentFactory componentFactory = mock(RetryTopicComponentFactory.class);
		DefaultDestinationTopicResolver resolverMock = mock(DefaultDestinationTopicResolver.class);
		given(componentFactory.destinationTopicResolver()).willReturn(resolverMock);
		Consumer<DestinationTopicResolver> dtrConsumer = mock(Consumer.class);
		RetryTopicConfigurationSupport support = new RetryTopicConfigurationSupport() {
			@Override
			protected RetryTopicComponentFactory createComponentFactory() {
				return componentFactory;
			}

			@Override
			protected Consumer<DestinationTopicResolver> configureDestinationTopicResolver() {
				return dtrConsumer;
			}

			@Override
			protected void manageNonBlockingFatalExceptions(List<Class<? extends Throwable>> nonBlockingRetries) {
				nonBlockingRetries.remove(ConversionException.class);
			}
		};
		DefaultDestinationTopicResolver resolver = (DefaultDestinationTopicResolver) support.destinationTopicResolver();
		assertThat(resolver).isEqualTo(resolverMock);
		then(dtrConsumer).should().accept(resolverMock);
		ArgumentCaptor<Map<Class<? extends Throwable>, Boolean>> captor = ArgumentCaptor.forClass(Map.class);
		then(resolverMock).should().setClassifications(captor.capture(), eq(true));
		assertThat(captor.getValue()).doesNotContainKey(ConversionException.class);
	}

	@Test
	void testCreateDestinationTopicResolverNoConfiguration() {
		RetryTopicConfigurationSupport support = new RetryTopicConfigurationSupport();
		DestinationTopicResolver resolver = support.destinationTopicResolver();
		assertThat(resolver).isNotNull();
	}

	@Test
	void testCreatesComponentFactory() {
		RetryTopicConfigurationSupport configurationSupport = new RetryTopicConfigurationSupport();
		assertThat(configurationSupport).hasFieldOrProperty("componentFactory").isNotNull();
	}

}
