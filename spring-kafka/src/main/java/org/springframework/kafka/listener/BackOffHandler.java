package org.springframework.kafka.listener;

import org.springframework.lang.Nullable;

/**
 * Handler for the provided back off time, listener container and exception.
 *
 *  @author Jan Marincek
 *
 *  @since 1.3.5
 */
@FunctionalInterface
public interface BackOffHandler {

	void onNextBackOff(@Nullable MessageListenerContainer container, Exception exception, long nextBackOff);

}
