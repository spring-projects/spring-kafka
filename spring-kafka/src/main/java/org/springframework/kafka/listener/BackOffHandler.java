package org.springframework.kafka.listener;

import org.springframework.lang.Nullable;

/**
 * Handler for the provided back off time, listener container and exception.
 *
 *  @author Jan Marincek
 *
 *  @since 1.3.5
 */
public interface BackOffHandler {

	default void onNextBackOff(@Nullable MessageListenerContainer container, Exception exception, long nextBackOff) {
		try {
			if (container == null) {
				Thread.sleep(nextBackOff);
			} else {
				ListenerUtils.stoppableSleep(container, nextBackOff);
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

}
