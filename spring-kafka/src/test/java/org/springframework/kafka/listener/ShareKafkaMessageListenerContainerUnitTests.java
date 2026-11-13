/*
 * Copyright 2025-present the original author or authors.
 */

package org.springframework.kafka.listener;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.ShareKafkaListenerContainerFactory;
import org.springframework.kafka.core.ShareConsumerFactory;
import org.springframework.kafka.event.ConsumerFailedToStartEvent;
import org.springframework.kafka.event.ConsumerStartedEvent;
import org.springframework.kafka.event.ConsumerStartingEvent;
import org.springframework.kafka.event.ConsumerStoppedEvent;
import org.springframework.kafka.event.ConsumerStoppedEvent.Reason;
import org.springframework.kafka.event.ShareConsumerStoppingEvent;
import org.springframework.kafka.listener.ContainerProperties.ShareAckMode;
import org.springframework.kafka.support.ShareAcknowledgment;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link ShareKafkaMessageListenerContainer}.
 * These tests focus on configuration validation and setup logic.
 * Message processing, acknowledgment behavior, and error handling are covered
 * by integration tests in {@link ShareKafkaMessageListenerContainerIntegrationTests}.
 *
 * @author Soby Chacko
 * @author Maxwell Balla
 * @author Kumar Gaurav
 * @author OhKyu Chan
 * @author Burak Kalayci
 * @author Jiyeon Kim
 * @author Ngoc Nhan
 * @author Minju Kim
 *
 * @since 4.0
 */
@ExtendWith(MockitoExtension.class)
public class ShareKafkaMessageListenerContainerUnitTests {

	@Mock
	private ShareConsumerFactory<String, String> shareConsumerFactory;

	@Mock
	private MessageListener<String, String> messageListener;

	@Mock
	private AcknowledgingShareConsumerAwareMessageListener<String, String> ackListener;

	@Test
	void shouldDefaultToExplicitAckMode() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThat(container.getContainerProperties().getShareAckMode()).isEqualTo(ShareAckMode.EXPLICIT);
	}

	@Test
	void shouldConfigureImplicitAckMode() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.IMPLICIT);
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThat(container.getContainerProperties().getShareAckMode()).isEqualTo(ShareAckMode.IMPLICIT);
	}

	@Test
	void shouldConfigureManualAckMode() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.MANUAL);
		containerProperties.setMessageListener(ackListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThat(container.getContainerProperties().getShareAckMode()).isEqualTo(ShareAckMode.MANUAL);
	}

	@Test
	void shouldFailWhenManualAckModeUsedWithNonAcknowledgingListener() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.MANUAL);
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(container::start)
				.withMessageContaining("ShareAckMode.MANUAL requires an AcknowledgingShareConsumerAwareMessageListener");
	}

	@Test
	void shouldNotFailWhenManualAckModeUsedWithAcknowledgingListener() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.MANUAL);
		containerProperties.setMessageListener(ackListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThat(container.getContainerProperties().getShareAckMode()).isEqualTo(ShareAckMode.MANUAL);
	}

	@Test
	@SuppressWarnings("deprecation")
	void deprecatedSetExplicitShareAcknowledgmentShouldDelegateToShareAckMode() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");

		containerProperties.setExplicitShareAcknowledgment(true);
		assertThat(containerProperties.getShareAckMode()).isEqualTo(ShareAckMode.MANUAL);
		assertThat(containerProperties.isExplicitShareAcknowledgment()).isTrue();

		containerProperties.setExplicitShareAcknowledgment(false);
		assertThat(containerProperties.getShareAckMode()).isEqualTo(ShareAckMode.EXPLICIT); // false maps to EXPLICIT (container-managed), not IMPLICIT
		assertThat(containerProperties.isExplicitShareAcknowledgment()).isFalse();
	}

	@Test
	void shouldRejectInvalidConcurrency() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThatIllegalArgumentException()
				.isThrownBy(() -> container.setConcurrency(0))
				.withMessageContaining("concurrency must be greater than 0");

		assertThatIllegalArgumentException()
				.isThrownBy(() -> container.setConcurrency(-1))
				.withMessageContaining("concurrency must be greater than 0");
	}

	@Test
	void shouldRejectTopicPatternAtConstruction() {
		ContainerProperties containerProperties = new ContainerProperties(Pattern.compile("test-.*"));

		assertThatIllegalArgumentException()
				.isThrownBy(() -> new ShareKafkaMessageListenerContainer<>(this.shareConsumerFactory,
						containerProperties))
				.withMessage("'topics' must be provided");
	}

	@Test
	void shouldRejectTopicPartitionsAtConstruction() {
		ContainerProperties containerProperties =
				new ContainerProperties(new TopicPartitionOffset("test-topic", 0));

		assertThatIllegalArgumentException()
				.isThrownBy(() -> new ShareKafkaMessageListenerContainer<>(this.shareConsumerFactory,
						containerProperties))
				.withMessage("'topics' must be provided");
	}

	@Test
	void shouldSupportBeanNameSetting() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		container.setBeanName("testContainer");
		assertThat(container.getBeanName()).isEqualTo("testContainer");
		assertThat(container.getListenerId()).isEqualTo("testContainer");
	}

	@Test
	void shouldSupportDefaultBeanNameSetting() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThat(container.getBeanName()).isEqualTo("noBeanNameSet");
		assertThat(container.getListenerId()).isEqualTo("noBeanNameSet");
	}

	@Test
	void shouldReportRunningStateBeforeStart() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThat(container.isRunning()).isFalse();
	}

	@Test
	void shouldUseRejectingRecovererByDefault() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThat(container.getShareConsumerRecordRecoverer()).isSameAs(ShareConsumerRecordRecoverer.REJECTING);
	}

	@Test
	void shouldAllowCustomShareConsumerRecordRecoverer() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		ShareConsumerRecordRecoverer customRecoverer = (record, ex) -> AcknowledgeType.RELEASE;
		container.setShareConsumerRecordRecoverer(customRecoverer);

		assertThat(container.getShareConsumerRecordRecoverer()).isSameAs(customRecoverer);
	}

	/**
	 * The programmatic entry point must configure the container the same way the
	 * endpoint-driven one does. {@code createListenerContainer(KafkaListenerEndpoint)}
	 * calls {@code initializeContainer(...)}; {@code createContainer(String...)} must
	 * too, otherwise every factory-level setting is silently dropped.
	 */
	@Test
	void createContainerByTopicsShouldApplyFactoryConfiguration() {
		ShareConsumerRecordRecoverer customRecoverer = (record, ex) -> AcknowledgeType.RELEASE;
		ApplicationEventPublisher publisher = mock(ApplicationEventPublisher.class);
		ShareKafkaListenerContainerFactory<String, String> factory =
				new ShareKafkaListenerContainerFactory<>(shareConsumerFactory);
		factory.setShareConsumerRecordRecoverer(customRecoverer);
		factory.setApplicationEventPublisher(publisher);
		factory.setAutoStartup(false);
		factory.setPhase(42);

		ShareKafkaMessageListenerContainer<String, String> container =
				factory.createContainer("test-topic");

		assertThat(container.getShareConsumerRecordRecoverer()).isSameAs(customRecoverer);
		assertThat(container.getApplicationEventPublisher()).isSameAs(publisher);
		assertThat(container.isAutoStartup()).isFalse();
		assertThat(container.getPhase()).isEqualTo(42);
	}

	@Test
	void factoryShouldApplyShareConsumerRecordRecovererToContainer() {
		ShareConsumerRecordRecoverer customRecoverer = (record, ex) -> AcknowledgeType.RELEASE;
		ShareKafkaListenerContainerFactory<String, String> factory =
				new ShareKafkaListenerContainerFactory<>(shareConsumerFactory);
		factory.setShareConsumerRecordRecoverer(customRecoverer);

		KafkaListenerEndpoint endpoint = mock(KafkaListenerEndpoint.class);
		given(endpoint.getTopics()).willReturn(java.util.List.of("test-topic"));
		given(endpoint.getConcurrency()).willReturn(null);

		ShareKafkaMessageListenerContainer<String, String> container =
				factory.createListenerContainer(endpoint);

		assertThat(container.getShareConsumerRecordRecoverer()).isSameAs(customRecoverer);
	}

	@Test
	void factoryShouldPropagateRecordMessageConverterToEndpoint() {
		RecordMessageConverter messageConverter = mock();
		ShareKafkaListenerContainerFactory<String, String> factory =
				new ShareKafkaListenerContainerFactory<>(shareConsumerFactory);
		factory.setRecordMessageConverter(messageConverter);

		KafkaListenerEndpoint endpoint = mock();
		given(endpoint.getTopics()).willReturn(List.of("test-topic"));
		given(endpoint.getConcurrency()).willReturn(null);

		ShareKafkaMessageListenerContainer<String, String> container = factory.createListenerContainer(endpoint);

		verify(endpoint).setupListenerContainer(container, messageConverter);
	}

	@Test
	void factoryShouldPreserveDefaultMessageConverterBehavior() {
		ShareKafkaListenerContainerFactory<String, String> factory =
				new ShareKafkaListenerContainerFactory<>(shareConsumerFactory);

		KafkaListenerEndpoint endpoint = mock();
		given(endpoint.getTopics()).willReturn(List.of("test-topic"));
		given(endpoint.getConcurrency()).willReturn(null);

		ShareKafkaMessageListenerContainer<String, String> container = factory.createListenerContainer(endpoint);

		verify(endpoint).setupListenerContainer(container, null);
	}

	/**
	 * Verifies that even when the factory has share.acknowledgement.mode=implicit in its
	 * base configuration, the container passes share.acknowledgement.mode=explicit as an
	 * override when creating the consumer. This is the critical contract: the container
	 * must enforce explicit mode regardless of what the factory is configured with.
	 */
	@Test
	@SuppressWarnings("unchecked")
	void shouldPassExplicitOverrideToConsumerEvenWhenFactoryConfigIsImplicit() {
		ShareConsumerFactory<String, String> conflictingFactory = mock(ShareConsumerFactory.class);
		given(conflictingFactory.getConfigurationProperties())
				.willReturn(Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "implicit"));

		ShareConsumer<String, String> mockConsumer = mock(ShareConsumer.class);
		given(conflictingFactory.createShareConsumer(any(), any(), any()))
				.willReturn(mockConsumer);
		lenient().when(mockConsumer.poll(any())).thenReturn(null);

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.EXPLICIT);
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(conflictingFactory, containerProperties);
		container.setBeanName("conflictWarningContainer");
		container.start();
		container.stop();

		verify(conflictingFactory).createShareConsumer(
				any(),
				any(),
				eq(Map.of(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit")));
	}

	@Test
	@SuppressWarnings("unchecked")
	void shouldUseClientIdFromContainerProperties() {
		ShareConsumerFactory<String, String> mockFactory = mock(ShareConsumerFactory.class);
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());
		ShareConsumer<String, String> mockConsumer = mock(ShareConsumer.class);
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);
		containerProperties.setClientId("share-client-from-properties");

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("clientIdFromPropertiesContainer");
		container.start();
		container.stop();

		verify(mockFactory).createShareConsumer(any(), eq("share-client-from-properties"), any());
	}

	@Test
	@SuppressWarnings("unchecked")
	void shouldPreferTheClientIdSetOnTheContainer() {
		ShareConsumerFactory<String, String> mockFactory = mock(ShareConsumerFactory.class);
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());
		ShareConsumer<String, String> mockConsumer = mock(ShareConsumer.class);
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);
		containerProperties.setClientId("share-client-from-properties");

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("clientIdOnContainerContainer");
		container.setClientId("share-client-on-container");
		container.start();
		container.stop();

		verify(mockFactory).createShareConsumer(any(), eq("share-client-on-container"), any());
	}

	@Test
	@SuppressWarnings("unchecked")
	void shouldFallBackToTheBeanNameWhenNoClientIdIsConfigured() {
		ShareConsumerFactory<String, String> mockFactory = mock(ShareConsumerFactory.class);
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());
		ShareConsumer<String, String> mockConsumer = mock(ShareConsumer.class);
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("beanNameFallbackContainer");
		container.start();
		container.stop();

		verify(mockFactory).createShareConsumer(any(), eq("beanNameFallbackContainer"), any());
	}

	@Test
	@SuppressWarnings("unchecked")
	void shouldUseCommitSyncByDefault() throws Exception {
		ShareConsumerFactory<String, String> mockFactory = mock(ShareConsumerFactory.class);
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());

		ShareConsumer<String, String> mockConsumer = mock(ShareConsumer.class);
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);

		ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
		ConsumerRecords<String, String> records = new ConsumerRecords<>(
				Map.of(new TopicPartition("test-topic", 0), List.of(record)), Map.of());

		AtomicBoolean firstPoll = new AtomicBoolean(true);
		given(mockConsumer.poll(any())).willAnswer(invocation -> {
			if (firstPoll.compareAndSet(true, false)) {
				return records;
			}
			Thread.sleep(50);
			return ConsumerRecords.empty();
		});

		CountDownLatch latch = new CountDownLatch(1);
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener((MessageListener<String, String>) rec -> latch.countDown());

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("syncCommitContainer");
		container.start();

		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();

		verify(mockConsumer).commitSync();
		verify(mockConsumer, never()).commitAsync();
	}

	@Test
	@SuppressWarnings("unchecked")
	void shouldUseCommitAsyncWhenSyncShareCommitsIsFalse() throws Exception {
		ShareConsumerFactory<String, String> mockFactory = mock(ShareConsumerFactory.class);
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());

		ShareConsumer<String, String> mockConsumer = mock(ShareConsumer.class);
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);

		ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
		ConsumerRecords<String, String> records = new ConsumerRecords<>(
				Map.of(new TopicPartition("test-topic", 0), List.of(record)), Map.of());

		AtomicBoolean firstPoll = new AtomicBoolean(true);
		given(mockConsumer.poll(any())).willAnswer(invocation -> {
			if (firstPoll.compareAndSet(true, false)) {
				return records;
			}
			Thread.sleep(50);
			return ConsumerRecords.empty();
		});

		CountDownLatch latch = new CountDownLatch(1);
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setSyncShareCommits(false);
		containerProperties.setMessageListener((MessageListener<String, String>) rec -> latch.countDown());

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("asyncCommitContainer");
		container.start();

		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		container.stop();

		verify(mockConsumer).commitAsync();
		verify(mockConsumer, never()).commitSync();
	}

	@Test
	void shouldAwaitConsumerThreadStartup() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);
		containerProperties.setConsumerStartTimeout(Duration.ofSeconds(5));
		containerProperties.setListenerTaskExecutor(task -> new Thread(() -> {
			try {
				Thread.sleep(200);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			task.run();
		}).start());

		@SuppressWarnings("unchecked")
		ShareConsumer<String, String> consumer = mock(ShareConsumer.class);
		given(this.shareConsumerFactory.createShareConsumer(any(), eq("startupWaitContainer"), any()))
				.willReturn(consumer);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(this.shareConsumerFactory, containerProperties);
		container.setBeanName("startupWaitContainer");

		long start = System.nanoTime();
		container.start();
		long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

		try {
			assertThat(elapsed).isGreaterThanOrEqualTo(150L);
		}
		finally {
			container.stop();
		}
	}

	@Test
	void shouldDefaultToNoAcknowledgementCommitCallback() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		assertThat(containerProperties.getAcknowledgementCommitCallback()).isNull();
	}

	@Test
	void shouldStoreCustomAcknowledgementCommitCallback() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		AcknowledgementCommitCallback customCallback = (offsets, exception) -> { };
		containerProperties.setAcknowledgementCommitCallback(customCallback);

		assertThat(containerProperties.getAcknowledgementCommitCallback()).isSameAs(customCallback);
	}

	@Test
	@SuppressWarnings("unchecked")
	void shouldRegisterCustomCallbackOnShareConsumer() throws Exception {
		ShareConsumerFactory<String, String> mockFactory = mock(ShareConsumerFactory.class);
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());

		ShareConsumer<String, String> mockConsumer = mock(ShareConsumer.class);
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);

		CountDownLatch callbackRegistered = new CountDownLatch(1);
		AcknowledgementCommitCallback customCallback = (offsets, exception) -> { };

		// Signal when callback is set on the consumer
		lenient().doAnswer(invocation -> {
			callbackRegistered.countDown();
			return null;
		}).when(mockConsumer).setAcknowledgementCommitCallback(any());

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setSyncShareCommits(false);
		containerProperties.setAcknowledgementCommitCallback(customCallback);
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("customCallbackContainer");
		container.start();
		assertThat(callbackRegistered.await(5, TimeUnit.SECONDS)).isTrue();
		container.stop();

		verify(mockConsumer).setAcknowledgementCommitCallback(customCallback);
	}

	@Test
	@SuppressWarnings("unchecked")
	void shouldRegisterDefaultLoggingCallbackWhenAsyncAndNoCustomCallback() throws Exception {
		ShareConsumerFactory<String, String> mockFactory = mock(ShareConsumerFactory.class);
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());

		ShareConsumer<String, String> mockConsumer = mock(ShareConsumer.class);
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);

		CountDownLatch callbackRegistered = new CountDownLatch(1);
		lenient().doAnswer(invocation -> {
			callbackRegistered.countDown();
			return null;
		}).when(mockConsumer).setAcknowledgementCommitCallback(any());

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setSyncShareCommits(false);
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("defaultCallbackContainer");
		container.start();

		assertThat(callbackRegistered.await(5, TimeUnit.SECONDS)).isTrue();

		container.stop();

		verify(mockConsumer).setAcknowledgementCommitCallback(any(AcknowledgementCommitCallback.class));
	}

	@Test
	@SuppressWarnings("unchecked")
	void shouldNotRegisterCallbackWhenSyncCommits() throws Exception {
		ShareConsumerFactory<String, String> mockFactory = mock(ShareConsumerFactory.class);
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());

		ShareConsumer<String, String> mockConsumer = mock(ShareConsumer.class);
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);

		CountDownLatch subscribed = new CountDownLatch(1);
		lenient().doAnswer(invocation -> {
			subscribed.countDown();
			return null;
		}).when(mockConsumer).subscribe(any(Collection.class));

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("noCallbackSyncContainer");
		container.start();

		assertThat(subscribed.await(5, TimeUnit.SECONDS)).isTrue();

		container.stop();

		verify(mockConsumer, never()).setAcknowledgementCommitCallback(any());
	}

	@SuppressWarnings("unchecked")
	@Test
	void shouldPublishStoppingAndStoppedEventsOnNormalShutdown() throws InterruptedException {
		ShareConsumer<String, String> consumer = mock(ShareConsumer.class);
		CountDownLatch pollEnteredLatch = new CountDownLatch(1);
		CountDownLatch pollReleaseLatch = new CountDownLatch(1);
		willAnswer(invocation -> {
			pollEnteredLatch.countDown();
			pollReleaseLatch.await(10, TimeUnit.SECONDS);
			return new ConsumerRecords<>(Collections.emptyMap(), Collections.emptyMap());
		}).given(consumer).poll(any());
		given(shareConsumerFactory.getConfigurationProperties()).willReturn(Map.of());
		given(shareConsumerFactory.createShareConsumer(any(), any(), any())).willReturn(consumer);

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		CountDownLatch stoppedLatch = new CountDownLatch(1);
		AtomicReference<Reason> stoppedReason = new AtomicReference<>();
		AtomicReference<ShareConsumer<?, ?>> stoppingConsumer = new AtomicReference<>();
		container.setApplicationEventPublisher(event -> {
			if (event instanceof ShareConsumerStoppingEvent stoppingEvent) {
				stoppingConsumer.set(stoppingEvent.getConsumer());
			}
			else if (event instanceof ConsumerStoppedEvent stoppedEvent) {
				stoppedReason.set(stoppedEvent.getReason());
				stoppedLatch.countDown();
			}
		});

		container.start();
		assertThat(pollEnteredLatch.await(10, TimeUnit.SECONDS)).isTrue();
		pollReleaseLatch.countDown();
		container.stop();

		assertThat(stoppedLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(stoppedReason.get()).isEqualTo(Reason.NORMAL);
		assertThat(stoppingConsumer.get()).isSameAs(consumer);
	}

	@SuppressWarnings("unchecked")
	@Test
	void shouldPublishLifecycleEventsInOrder() throws InterruptedException {
		ShareConsumer<String, String> consumer = mock(ShareConsumer.class);
		CountDownLatch pollEnteredLatch = new CountDownLatch(1);
		CountDownLatch pollReleaseLatch = new CountDownLatch(1);
		willAnswer(invocation -> {
			pollEnteredLatch.countDown();
			pollReleaseLatch.await(10, TimeUnit.SECONDS);
			return new ConsumerRecords<>(Collections.emptyMap(), Collections.emptyMap());
		}).given(consumer).poll(any());
		given(shareConsumerFactory.getConfigurationProperties()).willReturn(Map.of());
		given(shareConsumerFactory.createShareConsumer(any(), any(), any())).willReturn(consumer);

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		List<String> eventOrder = Collections.synchronizedList(new ArrayList<>());
		CountDownLatch stoppedLatch = new CountDownLatch(1);
		container.setApplicationEventPublisher(event -> {
			if (event instanceof ConsumerStartingEvent) {
				eventOrder.add("starting");
			}
			else if (event instanceof ConsumerStartedEvent) {
				eventOrder.add("started");
			}
			else if (event instanceof ShareConsumerStoppingEvent) {
				eventOrder.add("stopping");
			}
			else if (event instanceof ConsumerStoppedEvent) {
				eventOrder.add("stopped");
				stoppedLatch.countDown();
			}
		});

		container.start();
		assertThat(pollEnteredLatch.await(10, TimeUnit.SECONDS)).isTrue();
		pollReleaseLatch.countDown();
		container.stop();

		assertThat(stoppedLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(eventOrder).containsExactly("starting", "started", "stopping", "stopped");
	}

	@SuppressWarnings("unchecked")
	@Test
	void shouldPublishStoppedEventWithErrorReasonOnError() throws InterruptedException {
		ShareConsumer<String, String> consumer = mock(ShareConsumer.class);
		willAnswer(invocation -> {
			throw new OutOfMemoryError("test error");
		}).given(consumer).poll(any());
		given(shareConsumerFactory.getConfigurationProperties()).willReturn(Map.of());
		given(shareConsumerFactory.createShareConsumer(any(), any(), any())).willReturn(consumer);

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		CountDownLatch stoppedLatch = new CountDownLatch(1);
		AtomicReference<Reason> stoppedReason = new AtomicReference<>();
		container.setApplicationEventPublisher(event -> {
			if (event instanceof ConsumerStoppedEvent stoppedEvent) {
				stoppedReason.set(stoppedEvent.getReason());
				stoppedLatch.countDown();
			}
		});

		try {
			container.start();
			assertThat(stoppedLatch.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(stoppedReason.get()).isEqualTo(Reason.ERROR);
		}
		finally {
			container.stop();
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	void shouldPublishStoppedEventWithAbnormalReasonOnException() throws InterruptedException {
		ShareConsumer<String, String> consumer = mock(ShareConsumer.class);
		willAnswer(invocation -> {
			throw new RuntimeException("test exception");
		}).given(consumer).poll(any());
		given(shareConsumerFactory.getConfigurationProperties()).willReturn(Map.of());
		given(shareConsumerFactory.createShareConsumer(any(), any(), any())).willReturn(consumer);

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		CountDownLatch stoppedLatch = new CountDownLatch(1);
		AtomicReference<Reason> stoppedReason = new AtomicReference<>();
		container.setApplicationEventPublisher(event -> {
			if (event instanceof ConsumerStoppedEvent stoppedEvent) {
				stoppedReason.set(stoppedEvent.getReason());
				stoppedLatch.countDown();
			}
		});

		container.start();
		assertThat(stoppedLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(stoppedReason.get()).isEqualTo(Reason.ABNORMAL);
		container.stop();
	}

	@Test
	void shouldNotBeInExpectedStateAfterConsumerExitsOnException() throws InterruptedException {
		ShareConsumer<String, String> consumer = mock();
		given(consumer.poll(any()))
				.willThrow(new RuntimeException("test exception"))
				.willAnswer(invocation -> {
					Thread.sleep(10);
					return ConsumerRecords.empty();
				});
		given(shareConsumerFactory.getConfigurationProperties()).willReturn(Map.of());
		given(shareConsumerFactory.createShareConsumer(any(), any(), any())).willReturn(consumer);

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		CountDownLatch stoppedLatch = new CountDownLatch(1);
		container.setApplicationEventPublisher(event -> {
			if (event instanceof ConsumerStoppedEvent) {
				stoppedLatch.countDown();
			}
		});

		assertThat(container.isInExpectedState()).isTrue();
		container.start();
		assertThat(stoppedLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(container.isInExpectedState()).isFalse();
		container.stop();
		assertThat(container.isInExpectedState()).isFalse();
		container.start();
		assertThat(container.isInExpectedState()).isTrue();
		container.stop();
	}

	@Test
	void shouldNotBeInExpectedStateAfterConsumerExitsOnError() throws InterruptedException {
		ShareConsumer<String, String> consumer = mock();
		willAnswer(invocation -> {
			throw new StackOverflowError("test error");
		}).given(consumer).poll(any());
		given(shareConsumerFactory.getConfigurationProperties()).willReturn(Map.of());
		given(shareConsumerFactory.createShareConsumer(any(), any(), any())).willReturn(consumer);

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		CountDownLatch stoppedLatch = new CountDownLatch(1);
		container.setApplicationEventPublisher(event -> {
			if (event instanceof ConsumerStoppedEvent) {
				stoppedLatch.countDown();
			}
		});

		container.start();
		assertThat(stoppedLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(container.isInExpectedState()).isFalse();
		container.stop();
	}

	@Test
	void shouldNotBeInExpectedStateAfterStopAbnormally() throws InterruptedException {
		ShareConsumer<String, String> consumer = mock();
		CountDownLatch pollLatch = new CountDownLatch(1);
		given(consumer.poll(any())).willAnswer(invocation -> {
			pollLatch.countDown();
			Thread.sleep(10);
			return ConsumerRecords.empty();
		});
		given(shareConsumerFactory.getConfigurationProperties()).willReturn(Map.of());
		given(shareConsumerFactory.createShareConsumer(any(), any(), any())).willReturn(consumer);

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		container.start();
		assertThat(pollLatch.await(10, TimeUnit.SECONDS)).isTrue();
		CountDownLatch callbackLatch = new CountDownLatch(1);
		container.stopAbnormally(callbackLatch::countDown);
		assertThat(callbackLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(container.isRunning()).isFalse();
		assertThat(container.isInExpectedState()).isFalse();
		container.start();
		assertThat(container.isInExpectedState()).isTrue();
		container.stop();
		assertThat(container.isInExpectedState()).isTrue();
	}

	@Test
	void shouldNotBeInExpectedStateAfterStopAbnormallyDuringStart() throws Exception {
		ShareConsumer<String, String> consumer = mock();
		lenient().when(consumer.poll(any())).thenAnswer(invocation -> {
			Thread.sleep(10);
			return ConsumerRecords.empty();
		});
		CountDownLatch creatingLatch = new CountDownLatch(1);
		CountDownLatch releaseLatch = new CountDownLatch(1);
		given(shareConsumerFactory.getConfigurationProperties()).willReturn(Map.of());
		given(shareConsumerFactory.createShareConsumer(any(), any(), any())).willAnswer(invocation -> {
			creatingLatch.countDown();
			assertThat(releaseLatch.await(10, TimeUnit.SECONDS)).isTrue();
			return consumer;
		});

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor();
		CompletableFuture<Void> start = CompletableFuture.runAsync(container::start, executor);
		assertThat(creatingLatch.await(10, TimeUnit.SECONDS)).isTrue();
		CompletableFuture<Void> stopAbnormally = CompletableFuture.runAsync(() -> container.stopAbnormally(() -> {
		}), executor);
		await().atMost(Duration.ofSeconds(10)).until(container.lifecycleLock::hasQueuedThreads);
		releaseLatch.countDown();
		start.get(10, TimeUnit.SECONDS);
		stopAbnormally.get(10, TimeUnit.SECONDS);

		assertThat(container.isRunning()).isFalse();
		assertThat(container.isInExpectedState()).isFalse();
	}

	@Test
	void shouldBeInExpectedStateAfterNormalStop() throws InterruptedException {
		ShareConsumer<String, String> consumer = mock();
		CountDownLatch pollLatch = new CountDownLatch(1);
		given(consumer.poll(any())).willAnswer(invocation -> {
			pollLatch.countDown();
			Thread.sleep(10);
			return ConsumerRecords.empty();
		});
		given(shareConsumerFactory.getConfigurationProperties()).willReturn(Map.of());
		given(shareConsumerFactory.createShareConsumer(any(), any(), any())).willReturn(consumer);

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		container.start();
		assertThat(pollLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(container.isInExpectedState()).isTrue();
		container.stop();
		assertThat(container.isInExpectedState()).isTrue();
	}

	@Test
	void shouldPublishFailedToStartEventOnInitializationFailure() {
		given(shareConsumerFactory.getConfigurationProperties()).willReturn(Map.of());
		given(shareConsumerFactory.createShareConsumer(any(), any(), any()))
				.willThrow(new RuntimeException("failed to create consumer"));

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		AtomicReference<ConsumerFailedToStartEvent> failedEvent = new AtomicReference<>();
		container.setApplicationEventPublisher(event -> {
			if (event instanceof ConsumerFailedToStartEvent failed) {
				failedEvent.set(failed);
			}
		});

		assertThatExceptionOfType(RuntimeException.class)
				.isThrownBy(container::start)
				.withMessage("failed to create consumer");
		assertThat(failedEvent.get()).isNotNull();
	}

	@Test
	void shouldPublishFailedToStartWhenNotAllConsumersStartWithinTimeout() throws InterruptedException {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);
		containerProperties.setConsumerStartTimeout(Duration.ofMillis(50));
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(1);
		executor.setMaxPoolSize(1);
		executor.afterPropertiesSet();
		containerProperties.setListenerTaskExecutor(executor);

		@SuppressWarnings("unchecked")
		ShareConsumer<String, String> firstConsumer = mock(ShareConsumer.class);
		@SuppressWarnings("unchecked")
		ShareConsumer<String, String> secondConsumer = mock(ShareConsumer.class);
		given(this.shareConsumerFactory.createShareConsumer(any(), eq("failedStartContainer-0"), any()))
				.willReturn(firstConsumer);
		given(this.shareConsumerFactory.createShareConsumer(any(), eq("failedStartContainer-1"), any()))
				.willReturn(secondConsumer);
		lenient().doAnswer(invocation -> {
			Thread.sleep(200);
			return new ConsumerRecords<>(Collections.emptyMap(), Map.of());
		}).when(firstConsumer).poll(Duration.ofMillis(1000));

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(this.shareConsumerFactory, containerProperties);
		container.setBeanName("failedStartContainer");
		container.setConcurrency(2);
		CountDownLatch failedToStartLatch = new CountDownLatch(1);
		container.setApplicationEventPublisher(event -> {
			if (event instanceof ConsumerFailedToStartEvent) {
				failedToStartLatch.countDown();
			}
		});

		try {
			container.start();
			assertThat(failedToStartLatch.await(10, TimeUnit.SECONDS)).isTrue();
		}
		finally {
			container.stop();
			executor.destroy();
		}
	}

	@Test
	void shouldReturnEmptyClientInstanceIdsWhenNotRunning() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(shareConsumerFactory, containerProperties);

		assertThat(container.getClientInstanceIds(Duration.ZERO)).isEmpty();
	}

	@Test
	@SuppressWarnings("unchecked")
	void shouldReturnClientInstanceIdsForRunningConsumers() {
		ShareConsumerFactory<String, String> mockFactory = mock(ShareConsumerFactory.class);
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());

		ShareConsumer<String, String> mockConsumer = mock(ShareConsumer.class);
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);
		Uuid instanceId = Uuid.randomUuid();
		given(mockConsumer.clientInstanceId(any())).willReturn(instanceId);
		lenient().when(mockConsumer.poll(any())).thenAnswer(invocation -> {
			Thread.sleep(10);
			return ConsumerRecords.empty();
		});

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("clientInstanceIdContainer");
		container.start();
		try {
			Map<String, Uuid> instanceIds = container.getClientInstanceIds(Duration.ZERO);
			assertThat(instanceIds).hasSize(1).containsKey("clientInstanceIdContainer");
			assertThat(instanceIds.get("clientInstanceIdContainer")).isEqualTo(instanceId);
		}
		finally {
			container.stop();
		}
		assertThat(container.getClientInstanceIds(Duration.ZERO)).isEmpty();
	}

	@Test
	@SuppressWarnings("unchecked")
	void shouldPropagateClientInstanceIdException() {
		ShareConsumerFactory<String, String> mockFactory = mock(ShareConsumerFactory.class);
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());

		ShareConsumer<String, String> mockConsumer = mock(ShareConsumer.class);
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);
		given(mockConsumer.clientInstanceId(any())).willThrow(new IllegalStateException("telemetry disabled"));
		lenient().when(mockConsumer.poll(any())).thenAnswer(invocation -> {
			Thread.sleep(10);
			return ConsumerRecords.empty();
		});

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setMessageListener(messageListener);

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("clientInstanceIdErrorContainer");
		container.start();
		try {
			assertThatExceptionOfType(IllegalStateException.class)
					.isThrownBy(() -> container.getClientInstanceIds(Duration.ZERO))
					.withMessage("telemetry disabled");
		}
		finally {
			container.stop();
		}
	}

	@Test
	void terminalAcknowledgmentIsNotDowngradedByStaleRenewNotification() throws Exception {
		ShareAcknowledgment ack = captureManualAcknowledgment();
		ack.renew();
		ack.acknowledge();
		assertThat(KafkaTestUtils.getPropertyValue(ack, "acknowledgmentType", AtomicReference.class).get())
				.isEqualTo(AcknowledgeType.ACCEPT);

		// Exactly what processQueuedAcknowledgments() does when it drains the RENEW queued before the ACCEPT
		ReflectionTestUtils.invokeMethod(ack, "notifyAcknowledged", AcknowledgeType.RENEW);

		assertThat(KafkaTestUtils.getPropertyValue(ack, "acknowledgmentType", AtomicReference.class).get())
				.isEqualTo(AcknowledgeType.ACCEPT);
	}

	@Test
	void secondTerminalAcknowledgmentIsStillRejectedAfterStaleRenewNotification() throws Exception {
		ShareAcknowledgment ack = captureManualAcknowledgment();
		ack.renew();
		ack.acknowledge();
		ReflectionTestUtils.invokeMethod(ack, "notifyAcknowledged", AcknowledgeType.RENEW);

		assertThatIllegalStateException().isThrownBy(ack::reject);
	}

	@Test
	@SuppressWarnings("unchecked")
	void queuedAcknowledgmentIsDeliveredBeforeTheConsumerIsClosed() throws Exception {
		ShareConsumerFactory<String, String> mockFactory = mock(ShareConsumerFactory.class);
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());
		ShareConsumer<String, String> mockConsumer = mock(ShareConsumer.class);
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);

		ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
		ConsumerRecords<String, String> records = new ConsumerRecords<>(
				Map.of(new TopicPartition("test-topic", 0), List.of(record)), Map.of());
		AtomicBoolean firstPoll = new AtomicBoolean(true);
		given(mockConsumer.poll(any())).willAnswer(invocation -> {
			if (firstPoll.compareAndSet(true, false)) {
				return records;
			}
			Thread.sleep(50);
			return ConsumerRecords.empty();
		});

		CountDownLatch listenerEntered = new CountDownLatch(1);
		CountDownLatch releaseListener = new CountDownLatch(1);
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.MANUAL);
		containerProperties.setMessageListener((AcknowledgingShareConsumerAwareMessageListener<String, String>)
				(rec, acknowledgment, consumer) -> {
					listenerEntered.countDown();
					try {
						releaseListener.await(10, TimeUnit.SECONDS);
					}
					catch (InterruptedException ex) {
						Thread.currentThread().interrupt();
						return;
					}
					acknowledgment.acknowledge();
				});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("stopWithQueuedAckContainer");
		container.start();
		assertThat(listenerEntered.await(10, TimeUnit.SECONDS)).isTrue();

		// Stop while the consumer thread is still inside the listener, so the acknowledgment is
		// queued after the last drain: the loop exits straight to wrapUp() when the listener returns.
		Thread stopper = new Thread(container::stop);
		stopper.start();
		int waits = 0;
		while (container.isRunning() && waits++ < 1000) {
			Thread.sleep(10);
		}
		assertThat(container.isRunning()).isFalse();
		releaseListener.countDown();
		stopper.join(10_000);

		InOrder inOrder = inOrder(mockConsumer);
		inOrder.verify(mockConsumer).acknowledge(record, AcknowledgeType.ACCEPT);
		inOrder.verify(mockConsumer).close();
	}

	@Test
	void explicitModeAutoRenewIsDisabledByDefault() {
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		assertThat(containerProperties.isShareAutoRenewLocks()).isFalse();
	}

	@Test
	void explicitModeRenewsLockWhileRecordIsBeingProcessed() throws Exception {
		ShareConsumerFactory<String, String> mockFactory = mock();
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());
		ShareConsumer<String, String> mockConsumer = mock();
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);

		ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
		ExplicitModeShareConsumer client = new ExplicitModeShareConsumer(mockConsumer, List.of(record));

		CountDownLatch listenerEntered = new CountDownLatch(1);
		CountDownLatch releaseListener = new CountDownLatch(1);
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.EXPLICIT);
		containerProperties.setShareAutoRenewLocks(true);
		// Renewal interval is half the acknowledgment timeout: 200 ms
		containerProperties.setShareAcknowledgmentTimeout(Duration.ofMillis(400));
		containerProperties.setMessageListener((MessageListener<String, String>) rec -> {
			listenerEntered.countDown();
			try {
				releaseListener.await(10, TimeUnit.SECONDS);
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("autoRenewContainer");
		container.start();
		try {
			assertThat(listenerEntered.await(10, TimeUnit.SECONDS)).isTrue();
			// The record stays in flight well beyond the renewal interval, so the container
			// must renew its acquisition lock while processing is still in flight.
			await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
					verify(mockConsumer, atLeastOnce()).acknowledge(record, AcknowledgeType.RENEW));
			releaseListener.countDown();
			await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
					verify(mockConsumer).acknowledge(record, AcknowledgeType.ACCEPT));
			InOrder inOrder = inOrder(mockConsumer);
			inOrder.verify(mockConsumer).acknowledge(record, AcknowledgeType.RENEW);
			inOrder.verify(mockConsumer).acknowledge(record, AcknowledgeType.ACCEPT);
			assertThat(client.violations).isEmpty();
		}
		finally {
			releaseListener.countDown();
			container.stop();
		}
	}

	@Test
	void explicitModeDoesNotRenewLockByDefault() throws Exception {
		ShareConsumerFactory<String, String> mockFactory = mock();
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());
		ShareConsumer<String, String> mockConsumer = mock();
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);

		ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
		ConsumerRecords<String, String> records = new ConsumerRecords<>(
				Map.of(new TopicPartition("test-topic", 0), List.of(record)), Map.of());
		AtomicBoolean firstPoll = new AtomicBoolean(true);
		given(mockConsumer.poll(any())).willAnswer(invocation -> {
			if (firstPoll.compareAndSet(true, false)) {
				return records;
			}
			Thread.sleep(50);
			return ConsumerRecords.empty();
		});

		CountDownLatch listenerEntered = new CountDownLatch(1);
		CountDownLatch releaseListener = new CountDownLatch(1);
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.EXPLICIT);
		containerProperties.setShareAcknowledgmentTimeout(Duration.ofMillis(400));
		containerProperties.setMessageListener((MessageListener<String, String>) rec -> {
			listenerEntered.countDown();
			try {
				releaseListener.await(10, TimeUnit.SECONDS);
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("noAutoRenewContainer");
		container.start();
		try {
			assertThat(listenerEntered.await(10, TimeUnit.SECONDS)).isTrue();
			// Processing stays in flight for several renewal intervals; without the opt-in
			// the container must not send any RENEW (synchronous dispatch is unchanged).
			Thread.sleep(900);
			verify(mockConsumer, never()).acknowledge(record, AcknowledgeType.RENEW);
			releaseListener.countDown();
			await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
					verify(mockConsumer).acknowledge(record, AcknowledgeType.ACCEPT));
		}
		finally {
			releaseListener.countDown();
			container.stop();
		}
	}

	@Test
	void explicitModeAutoRenewAbandonsRecordWhoseRenewalFailedAndProcessesItsRedelivery() throws Exception {
		ShareConsumerFactory<String, String> mockFactory = mock();
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());
		ShareConsumer<String, String> mockConsumer = mock();
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);

		ConsumerRecord<String, String> slow = shareRecord(0, 0L, (short) 1, "slow");
		ConsumerRecord<String, String> lost = shareRecord(1, 0L, (short) 1, "lost");
		ExplicitModeShareConsumer client = new ExplicitModeShareConsumer(mockConsumer, List.of(slow, lost));
		// The renewal of the record on partition 1 fails, as when its lock already expired
		client.failingPartitions = Set.of(1);

		List<String> processed = Collections.synchronizedList(new ArrayList<>());
		CountDownLatch releaseSlow = new CountDownLatch(1);
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.EXPLICIT);
		containerProperties.setShareAutoRenewLocks(true);
		containerProperties.setShareAcknowledgmentTimeout(Duration.ofMillis(400));
		containerProperties.setMessageListener((MessageListener<String, String>) rec -> {
			processed.add(rec.value() + "/" + rec.deliveryCount().orElse((short) 0));
			if ("slow".equals(rec.value())) {
				try {
					releaseSlow.await(10, TimeUnit.SECONDS);
				}
				catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
			}
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("failedRenewalContainer");
		container.start();
		try {
			await().atMost(Duration.ofSeconds(10)).until(() -> client.acknowledgments.contains("RENEW@1:0"));
			await().atMost(Duration.ofSeconds(10)).until(() ->
					Collections.frequency(client.acknowledgments, "RENEW@0:0") >= 2);
			client.fetchable.add(shareRecord(1, 0L, (short) 2, "lost"));
			releaseSlow.countDown();

			await().atMost(Duration.ofSeconds(10)).until(() -> client.acknowledgments.contains("ACCEPT@1:0"));
			assertThat(processed).containsExactly("slow/1", "lost/2");
			assertThat(client.acknowledgments).contains("ACCEPT@0:0");
			assertThat(client.violations).isEmpty();
		}
		finally {
			releaseSlow.countDown();
			container.stop();
		}
	}

	@Test
	void explicitModeAutoRenewRejectsUndeserializableRecordPolledAfterRenewal() throws Exception {
		ShareConsumerFactory<String, String> mockFactory = mock();
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());
		ShareConsumer<String, String> mockConsumer = mock();
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);

		ConsumerRecord<String, String> record = shareRecord(0L, (short) 1, "payload");
		ExplicitModeShareConsumer client = new ExplicitModeShareConsumer(mockConsumer, List.of(record));
		client.pollErrorAfterCommit = new RecordDeserializationException(
				RecordDeserializationException.DeserializationExceptionOrigin.VALUE,
				new TopicPartition("test-topic", 0), 5L, ConsumerRecord.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE,
				null, null, new RecordHeaders(), "cannot deserialize", new IllegalArgumentException("bad payload"));

		CountDownLatch releaseListener = new CountDownLatch(1);
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.EXPLICIT);
		containerProperties.setShareAutoRenewLocks(true);
		containerProperties.setShareAcknowledgmentTimeout(Duration.ofMillis(400));
		containerProperties.setMessageListener((MessageListener<String, String>) rec -> {
			try {
				releaseListener.await(10, TimeUnit.SECONDS);
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("undeserializableAfterRenewalContainer");
		container.start();
		try {
			await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
					verify(mockConsumer).acknowledge("test-topic", 0, 5L, AcknowledgeType.REJECT));
			releaseListener.countDown();
			await().atMost(Duration.ofSeconds(10)).until(() -> client.acknowledgments.contains("ACCEPT@0:0"));
			assertThat(client.violations).isEmpty();
		}
		finally {
			releaseListener.countDown();
			container.stop();
		}
	}

	@Test
	void explicitModeAutoRenewDoesNotPollOrAcknowledgeWhileRenewalsCannotBeSent() throws Exception {
		ShareConsumerFactory<String, String> mockFactory = mock();
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());
		ShareConsumer<String, String> mockConsumer = mock();
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);

		ConsumerRecord<String, String> record = shareRecord(0L, (short) 1, "payload");
		ExplicitModeShareConsumer client = new ExplicitModeShareConsumer(mockConsumer, List.of(record));
		// Sending the first renewal times out, so the lock of the record cannot be trusted anymore
		client.commitsToFail.set(1);

		AtomicInteger invocations = new AtomicInteger();
		CountDownLatch releaseFirstDelivery = new CountDownLatch(1);
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.EXPLICIT);
		containerProperties.setShareAutoRenewLocks(true);
		containerProperties.setShareAcknowledgmentTimeout(Duration.ofMillis(400));
		containerProperties.setMessageListener((MessageListener<String, String>) rec -> {
			if (invocations.incrementAndGet() == 1) {
				try {
					releaseFirstDelivery.await(10, TimeUnit.SECONDS);
				}
				catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
			}
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("unconfirmedRenewalContainer");
		container.start();
		try {
			await().atMost(Duration.ofSeconds(10)).until(() -> client.acknowledgments.contains("RENEW@0:0"));
			releaseFirstDelivery.countDown();
			// The first delivery is abandoned; the record returned later is processed as a new delivery
			await().atMost(Duration.ofSeconds(10)).until(() -> client.acknowledgments.contains("ACCEPT@0:0"));
			assertThat(invocations.get()).isEqualTo(2);
			assertThat(Collections.frequency(client.acknowledgments, "ACCEPT@0:0")).isEqualTo(1);
			assertThat(client.violations).isEmpty();
		}
		finally {
			releaseFirstDelivery.countDown();
			container.stop();
		}
	}

	@Test
	void explicitModeAutoRenewDelegatesToRecovererOnError() throws Exception {
		ShareConsumerFactory<String, String> mockFactory = mock();
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());
		ShareConsumer<String, String> mockConsumer = mock();
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);

		ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
		ConsumerRecords<String, String> records = new ConsumerRecords<>(
				Map.of(new TopicPartition("test-topic", 0), List.of(record)), Map.of());
		AtomicBoolean firstPoll = new AtomicBoolean(true);
		given(mockConsumer.poll(any())).willAnswer(invocation -> {
			if (firstPoll.compareAndSet(true, false)) {
				return records;
			}
			Thread.sleep(50);
			return ConsumerRecords.empty();
		});

		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.EXPLICIT);
		containerProperties.setShareAutoRenewLocks(true);
		containerProperties.setMessageListener((MessageListener<String, String>) rec -> {
			throw new IllegalStateException("boom");
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("autoRenewErrorContainer");
		container.start();
		try {
			// The default recoverer rejects the record, as in the synchronous dispatch
			await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
					verify(mockConsumer).acknowledge(record, AcknowledgeType.REJECT));
			verify(mockConsumer, never()).acknowledge(record, AcknowledgeType.ACCEPT);
		}
		finally {
			container.stop();
		}
	}

	@Test
	void explicitModeAutoRenewDispatchesInOrderAndRenewsQueuedRecords() throws Exception {
		ShareConsumerFactory<String, String> mockFactory = mock();
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());
		ShareConsumer<String, String> mockConsumer = mock();
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);

		ConsumerRecord<String, String> first = new ConsumerRecord<>("test-topic", 0, 0L, "key", "first");
		ConsumerRecord<String, String> second = new ConsumerRecord<>("test-topic", 0, 1L, "key", "second");
		ExplicitModeShareConsumer client = new ExplicitModeShareConsumer(mockConsumer, List.of(first, second));

		List<String> processed = Collections.synchronizedList(new ArrayList<>());
		CountDownLatch releaseFirst = new CountDownLatch(1);
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.EXPLICIT);
		containerProperties.setShareAutoRenewLocks(true);
		containerProperties.setShareAcknowledgmentTimeout(Duration.ofMillis(400));
		containerProperties.setMessageListener((MessageListener<String, String>) rec -> {
			processed.add(rec.value());
			if ("first".equals(rec.value())) {
				try {
					releaseFirst.await(10, TimeUnit.SECONDS);
				}
				catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
			}
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("autoRenewOrderContainer");
		container.start();
		try {
			// The second record waits in the queue while the first is processed, so its lock is renewed too
			await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
					verify(mockConsumer, atLeastOnce()).acknowledge(second, AcknowledgeType.RENEW));
			assertThat(processed).containsExactly("first");
			releaseFirst.countDown();
			await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
					verify(mockConsumer).acknowledge(second, AcknowledgeType.ACCEPT));
			assertThat(processed).containsExactly("first", "second");
			InOrder inOrder = inOrder(mockConsumer);
			inOrder.verify(mockConsumer).acknowledge(first, AcknowledgeType.ACCEPT);
			inOrder.verify(mockConsumer).acknowledge(second, AcknowledgeType.ACCEPT);
			assertThat(client.violations).isEmpty();
		}
		finally {
			releaseFirst.countDown();
			container.stop();
		}
	}

	@Test
	void explicitModeAutoRenewCompletesInFlightRecordOnStop() throws Exception {
		ShareConsumerFactory<String, String> mockFactory = mock();
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());
		ShareConsumer<String, String> mockConsumer = mock();
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);

		ConsumerRecord<String, String> inFlight = new ConsumerRecord<>("test-topic", 0, 0L, "key", "in-flight");
		ConsumerRecord<String, String> queued = new ConsumerRecord<>("test-topic", 0, 1L, "key", "queued");
		ConsumerRecords<String, String> records = new ConsumerRecords<>(
				Map.of(new TopicPartition("test-topic", 0), List.of(inFlight, queued)), Map.of());
		AtomicBoolean firstPoll = new AtomicBoolean(true);
		given(mockConsumer.poll(any())).willAnswer(invocation -> {
			if (firstPoll.compareAndSet(true, false)) {
				return records;
			}
			Thread.sleep(50);
			return ConsumerRecords.empty();
		});

		CountDownLatch listenerEntered = new CountDownLatch(1);
		CountDownLatch releaseListener = new CountDownLatch(1);
		List<String> processed = Collections.synchronizedList(new ArrayList<>());
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.EXPLICIT);
		containerProperties.setShareAutoRenewLocks(true);
		containerProperties.setMessageListener((MessageListener<String, String>) rec -> {
			processed.add(rec.value());
			listenerEntered.countDown();
			try {
				releaseListener.await(10, TimeUnit.SECONDS);
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
		});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("autoRenewStopContainer");
		container.start();
		CompletableFuture<Void> stopFuture = null;
		try {
			assertThat(listenerEntered.await(10, TimeUnit.SECONDS)).isTrue();
			stopFuture = CompletableFuture.runAsync(container::stop);
			await().atMost(Duration.ofSeconds(10)).until(() -> !container.isRunning());
			releaseListener.countDown();
			stopFuture.get(10, TimeUnit.SECONDS);

			assertThat(processed).containsExactly("in-flight");
			InOrder inOrder = inOrder(mockConsumer);
			inOrder.verify(mockConsumer).acknowledge(inFlight, AcknowledgeType.ACCEPT);
			inOrder.verify(mockConsumer).acknowledge(queued, AcknowledgeType.RELEASE);
			inOrder.verify(mockConsumer).close();
			verify(mockConsumer, never()).acknowledge(queued, AcknowledgeType.ACCEPT);
		}
		finally {
			releaseListener.countDown();
			if (stopFuture == null) {
				container.stop();
			}
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void renewRestartsTheAcknowledgmentTimeout() throws Exception {
		ShareConsumerFactory<String, String> mockFactory = mock();
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());
		ShareConsumer<String, String> mockConsumer = mock();
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);

		ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
		ConsumerRecords<String, String> records = new ConsumerRecords<>(
				Map.of(new TopicPartition("test-topic", 0), List.of(record)), Map.of());
		AtomicBoolean firstPoll = new AtomicBoolean(true);
		given(mockConsumer.poll(any())).willAnswer(invocation -> {
			if (firstPoll.compareAndSet(true, false)) {
				return records;
			}
			Thread.sleep(50);
			return ConsumerRecords.empty();
		});

		AtomicReference<ShareAcknowledgment> captured = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(1);
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.MANUAL);
		containerProperties.setShareAcknowledgmentTimeout(Duration.ofHours(1));
		containerProperties.setMessageListener((AcknowledgingShareConsumerAwareMessageListener<String, String>)
				(rec, acknowledgment, consumer) -> {
					captured.set(acknowledgment);
					latch.countDown();
				});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("renewTimeoutContainer");
		container.start();
		try {
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			Object listenerConsumer = KafkaTestUtils.getPropertyValue(container, "consumers", List.class).get(0);
			Map<ConsumerRecord<?, ?>, Long> timestamps =
					KafkaTestUtils.getPropertyValue(listenerConsumer, "acknowledgmentTimestamps", Map.class);
			// Pretend the record was dispatched a while ago, still well within the timeout
			long dispatched = timestamps.get(record) - 1000;
			timestamps.put(record, dispatched);

			captured.get().renew();

			await().atMost(Duration.ofSeconds(10))
					.untilAsserted(() -> assertThat(timestamps.get(record)).isGreaterThan(dispatched));
			verify(mockConsumer).acknowledge(record, AcknowledgeType.RENEW);

			captured.get().acknowledge();

			await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(timestamps).isEmpty());
		}
		finally {
			container.stop();
		}
	}

	@Test
	void renewedRecordReturnedByPollIsNotDispatchedAgain() throws Exception {
		ShareConsumerFactory<String, String> mockFactory = mock();
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());
		ShareConsumer<String, String> mockConsumer = mock();
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);

		ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
		ConsumerRecord<String, String> renewed = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
		TopicPartition partition = new TopicPartition("test-topic", 0);
		AtomicInteger polls = new AtomicInteger();
		CountDownLatch renewedReturned = new CountDownLatch(1);
		given(mockConsumer.poll(any())).willAnswer(invocation -> {
			int poll = polls.incrementAndGet();
			if (poll == 1) {
				return new ConsumerRecords<>(Map.of(partition, List.of(record)), Map.of());
			}
			if (poll == 2) {
				renewedReturned.countDown();
				return new ConsumerRecords<>(Map.of(partition, List.of(renewed)), Map.of());
			}
			Thread.sleep(50);
			return ConsumerRecords.empty();
		});

		AtomicInteger invocations = new AtomicInteger();
		AtomicReference<ShareAcknowledgment> captured = new AtomicReference<>();
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.MANUAL);
		containerProperties.setMessageListener((AcknowledgingShareConsumerAwareMessageListener<String, String>)
				(rec, acknowledgment, consumer) -> {
					invocations.incrementAndGet();
					captured.set(acknowledgment);
				});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("renewedRecordContainer");
		container.start();
		try {
			assertThat(renewedReturned.await(10, TimeUnit.SECONDS)).isTrue();
			await().atMost(Duration.ofSeconds(10)).until(() -> polls.get() > 2);
			assertThat(invocations.get()).isEqualTo(1);

			captured.get().acknowledge();

			await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
					verify(mockConsumer).acknowledge(record, AcknowledgeType.ACCEPT));
			assertThat(invocations.get()).isEqualTo(1);
		}
		finally {
			container.stop();
		}
	}

	@Test
	void deferredRenewIsNotAppliedAfterLaterTerminalAcknowledgment() throws Exception {
		ShareConsumerFactory<String, String> mockFactory = mock();
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());
		ShareConsumer<String, String> mockConsumer = mock();
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);

		ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
		ConsumerRecords<String, String> records = new ConsumerRecords<>(
				Map.of(new TopicPartition("test-topic", 0), List.of(record)), Map.of());
		AtomicBoolean firstPoll = new AtomicBoolean(true);
		given(mockConsumer.poll(any())).willAnswer(invocation -> {
			if (firstPoll.compareAndSet(true, false)) {
				return records;
			}
			Thread.sleep(50);
			return ConsumerRecords.empty();
		});

		AtomicReference<ShareAcknowledgment> captured = new AtomicReference<>();
		List<AcknowledgeType> applied = Collections.synchronizedList(new ArrayList<>());
		AtomicBoolean rejectRenew = new AtomicBoolean(true);
		willAnswer(invocation -> {
			AcknowledgeType type = invocation.getArgument(1);
			if (type == AcknowledgeType.RENEW && rejectRenew.compareAndSet(true, false)) {
				// The listener completes processing while the consumer thread is applying the renewal
				captured.get().acknowledge();
				throw new IllegalStateException("The record cannot be acknowledged.");
			}
			applied.add(type);
			return null;
		}).given(mockConsumer).acknowledge(eq(record), any(AcknowledgeType.class));

		CountDownLatch latch = new CountDownLatch(1);
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.MANUAL);
		containerProperties.setMessageListener((AcknowledgingShareConsumerAwareMessageListener<String, String>)
				(rec, acknowledgment, consumer) -> {
					captured.set(acknowledgment);
					latch.countDown();
				});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("deferredRenewContainer");
		container.start();
		try {
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			Object listenerConsumer = KafkaTestUtils.getPropertyValue(container, "consumers", List.class).get(0);
			Map<ConsumerRecord<?, ?>, ?> pending =
					KafkaTestUtils.getPropertyValue(listenerConsumer, "pendingAcknowledgments", Map.class);

			captured.get().renew();

			await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(pending).isEmpty());
			Thread.sleep(200);
			assertThat(applied).containsExactly(AcknowledgeType.RENEW, AcknowledgeType.ACCEPT);
		}
		finally {
			container.stop();
		}
	}

	@Test
	void pendingRecordRedeliveredWithHigherDeliveryCountIsDispatchedAsNewDelivery() throws Exception {
		ShareConsumerFactory<String, String> mockFactory = mock();
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());
		ShareConsumer<String, String> mockConsumer = mock();
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);

		ConsumerRecord<String, String> firstDelivery = deliveredRecord((short) 1);
		ConsumerRecord<String, String> redelivery = deliveredRecord((short) 2);
		TopicPartition partition = new TopicPartition("test-topic", 0);
		AtomicBoolean firstPoll = new AtomicBoolean(true);
		CountDownLatch releaseRedelivery = new CountDownLatch(1);
		AtomicBoolean redelivered = new AtomicBoolean();
		given(mockConsumer.poll(any())).willAnswer(invocation -> {
			if (firstPoll.compareAndSet(true, false)) {
				return new ConsumerRecords<>(Map.of(partition, List.of(firstDelivery)), Map.of());
			}
			if (releaseRedelivery.getCount() == 0 && redelivered.compareAndSet(false, true)) {
				return new ConsumerRecords<>(Map.of(partition, List.of(redelivery)), Map.of());
			}
			Thread.sleep(50);
			return ConsumerRecords.empty();
		});
		// The lock of the first delivery expired, so the client rejects its acknowledgment
		AtomicInteger firstDeliveryAttempts = new AtomicInteger();
		willAnswer(invocation -> {
			firstDeliveryAttempts.incrementAndGet();
			throw new IllegalStateException("The record cannot be acknowledged.");
		}).given(mockConsumer).acknowledge(firstDelivery, AcknowledgeType.ACCEPT);

		List<ShareAcknowledgment> acknowledgments = Collections.synchronizedList(new ArrayList<>());
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.MANUAL);
		containerProperties.setMessageListener((AcknowledgingShareConsumerAwareMessageListener<String, String>)
				(rec, acknowledgment, consumer) -> acknowledgments.add(acknowledgment));

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("redeliveryContainer");
		container.start();
		try {
			await().atMost(Duration.ofSeconds(10)).until(() -> acknowledgments.size() == 1);
			acknowledgments.get(0).acknowledge();
			await().atMost(Duration.ofSeconds(10)).until(() -> firstDeliveryAttempts.get() > 0);

			releaseRedelivery.countDown();

			await().atMost(Duration.ofSeconds(10)).until(() -> acknowledgments.size() == 2);
			Object listenerConsumer = KafkaTestUtils.getPropertyValue(container, "consumers", List.class).get(0);
			Map<ConsumerRecord<?, ?>, ?> pending =
					KafkaTestUtils.getPropertyValue(listenerConsumer, "pendingAcknowledgments", Map.class);
			assertThat(pending.keySet()).containsExactly(redelivery);
			assertThat(KafkaTestUtils.getPropertyValue(listenerConsumer, "deferredAcknowledgments", List.class))
					.isEmpty();
			int attemptsAfterRetirement = firstDeliveryAttempts.get();

			acknowledgments.get(1).acknowledge();

			await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
					verify(mockConsumer).acknowledge(redelivery, AcknowledgeType.ACCEPT));
			await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(pending).isEmpty());
			assertThat(firstDeliveryAttempts.get()).isEqualTo(attemptsAfterRetirement);
		}
		finally {
			container.stop();
		}
	}

	private static ConsumerRecord<String, String> shareRecord(long offset, short deliveryCount, String value) {
		return shareRecord(0, offset, deliveryCount, value);
	}

	private static ConsumerRecord<String, String> shareRecord(int partition, long offset, short deliveryCount,
			String value) {

		return new ConsumerRecord<>("test-topic", partition, offset, ConsumerRecord.NO_TIMESTAMP,
				TimestampType.NO_TIMESTAMP_TYPE, ConsumerRecord.NULL_SIZE, ConsumerRecord.NULL_SIZE, "key", value,
				new RecordHeaders(), Optional.empty(), Optional.of(deliveryCount));
	}

	private static ConsumerRecord<String, String> deliveredRecord(short deliveryCount) {
		return new ConsumerRecord<>("test-topic", 0, 0L, ConsumerRecord.NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE,
				ConsumerRecord.NULL_SIZE, ConsumerRecord.NULL_SIZE, "key", "value", new RecordHeaders(),
				Optional.empty(), Optional.of(deliveryCount));
	}

	@Test
	void acknowledgmentRejectedDuringRenewalIsRetriedAfterPoll() throws Exception {
		ShareConsumerFactory<String, String> mockFactory = mock();
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());
		ShareConsumer<String, String> mockConsumer = mock();
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);

		ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
		ConsumerRecords<String, String> records = new ConsumerRecords<>(
				Map.of(new TopicPartition("test-topic", 0), List.of(record)), Map.of());
		List<String> events = Collections.synchronizedList(new ArrayList<>());
		AtomicBoolean firstPoll = new AtomicBoolean(true);
		given(mockConsumer.poll(any())).willAnswer(invocation -> {
			events.add("poll");
			if (firstPoll.compareAndSet(true, false)) {
				return records;
			}
			Thread.sleep(50);
			return ConsumerRecords.empty();
		});
		AtomicBoolean rejectAccept = new AtomicBoolean(true);
		willAnswer(invocation -> {
			if (rejectAccept.compareAndSet(true, false)) {
				events.add("accept-rejected");
				throw new IllegalStateException("The record cannot be acknowledged.");
			}
			events.add("accept");
			return null;
		}).given(mockConsumer).acknowledge(record, AcknowledgeType.ACCEPT);

		AtomicInteger invocations = new AtomicInteger();
		AtomicReference<ShareAcknowledgment> captured = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(1);
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.MANUAL);
		containerProperties.setMessageListener((AcknowledgingShareConsumerAwareMessageListener<String, String>)
				(rec, acknowledgment, consumer) -> {
					invocations.incrementAndGet();
					captured.set(acknowledgment);
					latch.countDown();
				});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("deferredAckContainer");
		container.start();
		try {
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			Object listenerConsumer = KafkaTestUtils.getPropertyValue(container, "consumers", List.class).get(0);
			Map<ConsumerRecord<?, ?>, ?> pending =
					KafkaTestUtils.getPropertyValue(listenerConsumer, "pendingAcknowledgments", Map.class);

			captured.get().acknowledge();

			await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(pending).isEmpty());
			List<String> snapshot;
			synchronized (events) {
				snapshot = new ArrayList<>(events);
			}
			int rejected = snapshot.indexOf("accept-rejected");
			int accepted = snapshot.indexOf("accept");
			assertThat(rejected).isNotNegative();
			assertThat(accepted).isGreaterThan(rejected);
			assertThat(snapshot.subList(rejected, accepted)).contains("poll");
			await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
				synchronized (events) {
					assertThat(events.subList(events.indexOf("accept"), events.size())).contains("poll");
				}
			});
			assertThat(Collections.frequency(snapshot, "accept")).isEqualTo(1);
			assertThat(invocations.get()).isEqualTo(1);
		}
		finally {
			container.stop();
		}
	}

	@SuppressWarnings("unchecked")
	private ShareAcknowledgment captureManualAcknowledgment() throws Exception {
		ShareConsumerFactory<String, String> mockFactory = mock(ShareConsumerFactory.class);
		given(mockFactory.getConfigurationProperties()).willReturn(Map.of());
		ShareConsumer<String, String> mockConsumer = mock(ShareConsumer.class);
		given(mockFactory.createShareConsumer(any(), any(), any())).willReturn(mockConsumer);

		ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
		ConsumerRecords<String, String> records = new ConsumerRecords<>(
				Map.of(new TopicPartition("test-topic", 0), List.of(record)), Map.of());
		AtomicBoolean firstPoll = new AtomicBoolean(true);
		given(mockConsumer.poll(any())).willAnswer(invocation -> {
			if (firstPoll.compareAndSet(true, false)) {
				return records;
			}
			Thread.sleep(50);
			return ConsumerRecords.empty();
		});

		AtomicReference<ShareAcknowledgment> captured = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(1);
		ContainerProperties containerProperties = new ContainerProperties("test-topic");
		containerProperties.setShareAckMode(ShareAckMode.MANUAL);
		containerProperties.setMessageListener((AcknowledgingShareConsumerAwareMessageListener<String, String>)
				(rec, acknowledgment, consumer) -> {
					captured.set(acknowledgment);
					latch.countDown();
				});

		ShareKafkaMessageListenerContainer<String, String> container =
				new ShareKafkaMessageListenerContainer<>(mockFactory, containerProperties);
		container.setBeanName("staleRenewContainer");
		container.start();
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		// Stop first so the consumer thread no longer drains the queue; everything after this is deterministic.
		container.stop();

		ShareAcknowledgment ack = captured.get();
		assertThat(ack).isNotNull();
		return ack;
	}

	/**
	 * Answers a mock share consumer the way the client behaves in explicit acknowledgement mode.
	 * Acknowledgments are sent on poll(), commitSync() or commitAsync(). A record with a RENEW
	 * sent cannot be acknowledged until poll() returns it; the renewal completes when sent by
	 * commitSync(), otherwise on the next poll(), and a completed renewal is returned by the
	 * next poll() without new records being fetched. A failed renewal is reported by
	 * commitSync() for the partition and the record is no longer acquired. A commitSync() that
	 * fails leaves its renewals in progress for a while. Rule violations are recorded, including a
	 * poll() that sends renewals or is called while renewals are in progress: the client then
	 * acquires new records that it does not return until the renewals complete.
	 */
	private static final class ExplicitModeShareConsumer {

		private final List<ConsumerRecord<String, String>> batch;

		private final Map<String, ConsumerRecord<String, String>> acquired = new LinkedHashMap<>();

		private final Map<String, AcknowledgeType> unsent = new LinkedHashMap<>();

		private final Map<String, ConsumerRecord<String, String>> renewalsInProgress = new LinkedHashMap<>();

		private final Map<String, ConsumerRecord<String, String>> renewalsOfFailedCommit = new LinkedHashMap<>();

		private long failedCommitRenewalsCompleteAt;

		private final Map<String, ConsumerRecord<String, String>> renewed = new LinkedHashMap<>();

		private final Queue<ConsumerRecord<String, String>> fetchable = new ConcurrentLinkedQueue<>();

		private final List<String> acknowledgments = Collections.synchronizedList(new ArrayList<>());

		private final List<String> violations = Collections.synchronizedList(new ArrayList<>());

		private volatile Set<Integer> failingPartitions = Set.of();

		private final AtomicInteger commitsToFail = new AtomicInteger();

		// Thrown by the first poll() after the next successful commitSync()
		private volatile @Nullable RuntimeException pollErrorAfterCommit;

		private @Nullable RuntimeException armedPollError;

		private boolean polled;

		ExplicitModeShareConsumer(ShareConsumer<String, String> consumer, List<ConsumerRecord<String, String>> batch) {
			this.batch = batch;
			given(consumer.poll(any())).willAnswer(invocation -> poll());
			willAnswer(invocation -> {
				acknowledge(invocation.getArgument(0), invocation.getArgument(1));
				return null;
			}).given(consumer).acknowledge(ArgumentMatchers.<ConsumerRecord<String, String>>any(),
					any(AcknowledgeType.class));
			lenient().doAnswer(invocation -> commitSync()).when(consumer).commitSync();
			lenient().doAnswer(invocation -> {
				send(false);
				return null;
			}).when(consumer).commitAsync();
		}

		private static String key(ConsumerRecord<String, String> record) {
			return record.partition() + ":" + record.offset();
		}

		private synchronized void acknowledge(ConsumerRecord<String, String> record, AcknowledgeType type) {
			String key = key(record);
			if (!this.acquired.containsKey(key)) {
				this.violations.add(type.name() + " for " + key + ", which cannot be acknowledged");
				throw new IllegalStateException("The record cannot be acknowledged.");
			}
			this.unsent.put(key, type);
			this.acknowledgments.add(type.name() + "@" + key);
		}

		private synchronized Set<Integer> send(boolean synchronous) {
			Set<Integer> failed = new HashSet<>();
			this.unsent.forEach((key, type) -> {
				ConsumerRecord<String, String> record = this.acquired.remove(key);
				if (type == AcknowledgeType.RENEW) {
					if (this.failingPartitions.contains(record.partition())) {
						failed.add(record.partition());
					}
					else if (synchronous) {
						this.renewed.put(key, record);
					}
					else {
						this.renewalsInProgress.put(key, record);
					}
				}
			});
			this.unsent.clear();
			return failed;
		}

		private synchronized Map<TopicIdPartition, Optional<KafkaException>> commitSync() {
			if (this.commitsToFail.getAndUpdate(count -> Math.max(0, count - 1)) > 0) {
				send(false);
				this.renewalsOfFailedCommit.putAll(this.renewalsInProgress);
				this.renewalsInProgress.clear();
				this.failedCommitRenewalsCompleteAt = System.currentTimeMillis() + 1000;
				throw new org.apache.kafka.common.errors.TimeoutException("Timeout waiting for acknowledgements");
			}
			this.armedPollError = this.pollErrorAfterCommit;
			this.pollErrorAfterCommit = null;
			Map<TopicIdPartition, Optional<KafkaException>> result = new HashMap<>();
			send(true).forEach(partition -> result.put(new TopicIdPartition(Uuid.ZERO_UUID, partition, "test-topic"),
					Optional.of(new KafkaException("renewal failed"))));
			return result;
		}

		private ConsumerRecords<String, String> poll() throws InterruptedException {
			RuntimeException error;
			synchronized (this) {
				error = this.armedPollError;
				this.armedPollError = null;
			}
			if (error != null) {
				throw error;
			}
			ConsumerRecords<String, String> records = fetch();
			if (records.isEmpty()) {
				Thread.sleep(20);
			}
			return records;
		}

		private synchronized ConsumerRecords<String, String> fetch() {
			if (!this.polled) {
				this.polled = true;
				return acquire(this.batch);
			}
			for (String key : this.acquired.keySet()) {
				if (!this.unsent.containsKey(key)) {
					this.violations.add("poll() with unacknowledged " + key);
					throw new IllegalStateException("All records must be acknowledged in explicit acknowledgement mode.");
				}
			}
			if (!this.renewalsOfFailedCommit.isEmpty()) {
				if (System.currentTimeMillis() < this.failedCommitRenewalsCompleteAt) {
					this.violations.add("poll() while renewals " + this.renewalsOfFailedCommit.keySet()
							+ " are in progress acquires new records");
				}
				this.renewed.putAll(this.renewalsOfFailedCommit);
				this.renewalsOfFailedCommit.clear();
			}
			// Renewals sent by the previous poll() complete now
			this.renewed.putAll(this.renewalsInProgress);
			this.renewalsInProgress.clear();
			send(false);
			if (!this.renewalsInProgress.isEmpty()) {
				this.violations.add("poll() sent renewals " + this.renewalsInProgress.keySet()
						+ " and acquires new records while they are in progress");
			}
			if (!this.renewed.isEmpty()) {
				List<ConsumerRecord<String, String>> returned = new ArrayList<>(this.renewed.values());
				this.renewed.clear();
				return acquire(returned);
			}
			if (this.renewalsInProgress.isEmpty() && this.acquired.isEmpty() && !this.fetchable.isEmpty()) {
				List<ConsumerRecord<String, String>> fetched = new ArrayList<>(this.fetchable);
				this.fetchable.clear();
				return acquire(fetched);
			}
			return ConsumerRecords.empty();
		}

		private ConsumerRecords<String, String> acquire(List<ConsumerRecord<String, String>> records) {
			if (records.isEmpty()) {
				return ConsumerRecords.empty();
			}
			Map<TopicPartition, List<ConsumerRecord<String, String>>> byPartition = new LinkedHashMap<>();
			records.forEach(record -> {
				this.acquired.put(key(record), record);
				byPartition.computeIfAbsent(new TopicPartition(record.topic(), record.partition()),
						partition -> new ArrayList<>()).add(record);
			});
			return new ConsumerRecords<>(byPartition, Map.of());
		}

	}

}
