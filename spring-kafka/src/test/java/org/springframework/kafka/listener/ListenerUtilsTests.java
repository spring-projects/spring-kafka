/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.kafka.listener;

import java.util.Optional;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * @author Gary Russell
 * @author Francois Rosiere
 * @author Antonio Tomac
 * @since 2.7.1
 *
 */
public class ListenerUtilsTests {

	@ParameterizedTest(name = "{0}")
	@MethodSource("leaderEpochCompatibility")
	void recordEpochRespectsProviderResult(String scenario, OffsetAndMetadata provided,
			Optional<Integer> sourceEpoch, OffsetAndMetadata expected) {

		MessageListenerContainer container = mock(MessageListenerContainer.class);
		ContainerProperties properties = new ContainerProperties("foo");
		OffsetAndMetadataProvider provider = mock(OffsetAndMetadataProvider.class);
		properties.setOffsetAndMetadataProvider(provider);
		given(container.getContainerProperties()).willReturn(properties);
		given(provider.provide(any(), eq(42L))).willReturn(provided);
		OffsetAndMetadata result = ListenerUtils.createOffsetAndMetadata(container, 42L, sourceEpoch);
		assertThat(result).isEqualTo(expected);
		if (expected == provided) {
			assertThat(result).isSameAs(provided);
		}
		verify(provider).provide(any(), eq(42L));
		verifyNoMoreInteractions(provider);
	}

	static Stream<Arguments> leaderEpochCompatibility() {
		OffsetAndMetadata unchanged = new OffsetAndMetadata(42L, "custom");
		OffsetAndMetadata explicit = new OffsetAndMetadata(42L, Optional.of(9), "custom");
		OffsetAndMetadata changed = new OffsetAndMetadata(100L, "custom");
		OffsetAndMetadata changedExplicit = new OffsetAndMetadata(100L, Optional.of(9), "custom");
		OffsetAndMetadata explicitZero = new OffsetAndMetadata(42L, Optional.of(0), "custom");
		return Stream.of(
				Arguments.of("fill missing epoch", unchanged, Optional.of(7),
						new OffsetAndMetadata(42L, Optional.of(7), "custom")),
				Arguments.of("provider epoch wins", explicit, Optional.of(7), explicit),
				Arguments.of("changed offset stays unknown", changed, Optional.of(7), changed),
				Arguments.of("changed offset keeps epoch", changedExplicit, Optional.of(7), changedExplicit),
				Arguments.of("unknown stays unknown", unchanged, Optional.empty(), unchanged),
				Arguments.of("zero is known", unchanged, Optional.of(0),
						new OffsetAndMetadata(42L, Optional.of(0), "custom")),
				Arguments.of("provider zero wins", explicitZero, Optional.of(7), explicitZero),
				Arguments.of("provider epoch survives unknown source", explicit, Optional.empty(), explicit));
	}

	@Test
	void legacyOffsetOnlyOverloadReturnsProviderResultWithoutEnrichment() {
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		ContainerProperties properties = new ContainerProperties("foo");
		OffsetAndMetadataProvider provider = mock(OffsetAndMetadataProvider.class);
		OffsetAndMetadata provided = new OffsetAndMetadata(100L, "custom");
		properties.setOffsetAndMetadataProvider(provider);
		given(container.getContainerProperties()).willReturn(properties);
		given(provider.provide(any(), eq(42L))).willReturn(provided);
		assertThat(ListenerUtils.createOffsetAndMetadata(container, 42L)).isSameAs(provided);
		verify(provider).provide(any(), eq(42L));
		verifyNoMoreInteractions(provider);
	}

	@Test
	void defaultProviderPreservesKnownEpoch() {
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		given(container.getContainerProperties()).willReturn(new ContainerProperties("foo"));
		assertThat(ListenerUtils.createOffsetAndMetadata(container, 42L, Optional.of(7)))
				.isEqualTo(new OffsetAndMetadata(42L, Optional.of(7), ""));
	}

	@Test
	void epochAwareHelperPropagatesProviderException() {
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		ContainerProperties properties = new ContainerProperties("foo");
		IllegalStateException failure = new IllegalStateException("provider failure");
		properties.setOffsetAndMetadataProvider((metadata, offset) -> {
			throw failure;
		});
		given(container.getContainerProperties()).willReturn(properties);
		assertThatThrownBy(() -> ListenerUtils.createOffsetAndMetadata(container, 42L, Optional.of(7)))
				.isSameAs(failure);
	}

	@Test
	void stoppableSleep() throws InterruptedException {
		MessageListenerContainer container = mock(MessageListenerContainer.class);
		given(container.isRunning()).willReturn(true);
		long t1 = System.currentTimeMillis();
		ListenerUtils.stoppableSleep(container, 500);
		assertThat(System.currentTimeMillis() - t1).isGreaterThanOrEqualTo(500);
	}

	@Test
	void conditionalSleepWithConditionTrue() throws InterruptedException {
		long t1 = System.currentTimeMillis();
		ListenerUtils.conditionalSleep(() -> true, 500);
		assertThat(System.currentTimeMillis() - t1).isGreaterThanOrEqualTo(500);
	}

	@Test
	void conditionalSleepWithConditionFalse() throws InterruptedException {
		long t1 = System.currentTimeMillis();
		ListenerUtils.conditionalSleep(() -> false, 500);
		assertThat(System.currentTimeMillis() - t1).isLessThan(500);
	}

	@Test
	void testCreationOfOffsetAndMetadataWithoutProvider() {
		final MessageListenerContainer container = mock(MessageListenerContainer.class);
		given(container.getContainerProperties()).willReturn(new ContainerProperties("foo"));
		final OffsetAndMetadata offsetAndMetadata = ListenerUtils.createOffsetAndMetadata(container, 1L);
		assertThat(offsetAndMetadata.offset()).isEqualTo(1);
		assertThat(offsetAndMetadata.metadata()).isEmpty();
	}

	@Test
	void testCreationOfOffsetAndMetadataWithProvider() {
		final MessageListenerContainer container = mock(MessageListenerContainer.class);
		final ContainerProperties properties = new ContainerProperties("foo");
		properties.setOffsetAndMetadataProvider((listenerMetadata, offset) -> new OffsetAndMetadata(offset, "my-metadata"));
		given(container.getContainerProperties()).willReturn(properties);
		final OffsetAndMetadata offsetAndMetadata = ListenerUtils.createOffsetAndMetadata(container, 1L);
		assertThat(offsetAndMetadata.offset()).isEqualTo(1);
		assertThat(offsetAndMetadata.metadata()).isEqualTo("my-metadata");
	}
}

