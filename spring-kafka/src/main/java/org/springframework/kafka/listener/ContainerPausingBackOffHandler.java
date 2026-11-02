/*
 * Copyright 2022-present the original author or authors.
 */

package org.springframework.kafka.listener;

import java.time.Duration;

import org.apache.kafka.common.TopicPartition;
import org.jspecify.annotations.Nullable;

/**
 * A {@link BackOffHandler} that pauses the container for the requested back off period.
 *
 * @author Gary Russell
 * @since 2.9
 *
 */
public class ContainerPausingBackOffHandler implements BackOffHandler {

	private final DefaultBackOffHandler defaultBackOffHandler = new DefaultBackOffHandler();

	private final ListenerContainerPauseService pauser;

	/**
	 * Create an instance with the provided {@link ListenerContainerPauseService}.
	 * @param pauser the pause service.
	 */
	public ContainerPausingBackOffHandler(ListenerContainerPauseService pauser) {
		this.pauser = pauser;
	}

	@Override
	public void onNextBackOff(@Nullable MessageListenerContainer container, @Nullable Exception exception, long nextBackOff) {
		if (container == null) {
			this.defaultBackOffHandler.onNextBackOff(container, exception, nextBackOff); // NOSONAR
		}
		else {
			this.pauser.pause(container, Duration.ofMillis(nextBackOff));
		}
	}

	@Override
	public void onNextBackOff(MessageListenerContainer container, TopicPartition partition, long nextBackOff) {
		this.pauser.pausePartition(container, partition, Duration.ofMillis(nextBackOff));
	}

}
