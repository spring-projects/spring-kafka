/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

/**
 * @author Gary Russell
 * @author Francois Rosiere
 * @author Antonio Tomac
 * @since 2.7.1
 *
 */
public class ListenerUtilsTests {

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

