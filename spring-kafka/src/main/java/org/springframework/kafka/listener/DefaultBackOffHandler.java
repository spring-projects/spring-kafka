/*
 * Copyright 2022-present the original author or authors.
 */

package org.springframework.kafka.listener;

import org.jspecify.annotations.Nullable;

/**
 * Default {@link BackOffHandler}; suspends the thread for the back off. If a container is
 * provided, {@link ListenerUtils#stoppableSleep(MessageListenerContainer, long)} is used,
 * to terminate the suspension if the container is stopped.
 *
 * @author Jan Marincek
 * @author Gary Russell
 * @since 2.9
 */
public class DefaultBackOffHandler implements BackOffHandler {

	@Override
	public void onNextBackOff(@Nullable MessageListenerContainer container, @Nullable Exception exception, long nextBackOff) {
		try {
			if (container == null) {
				Thread.sleep(nextBackOff);
			}
			else {
				ListenerUtils.stoppableSleep(container, nextBackOff);
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

}
