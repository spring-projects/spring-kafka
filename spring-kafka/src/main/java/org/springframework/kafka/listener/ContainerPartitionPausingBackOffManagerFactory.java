/*
 * Copyright 2022-present the original author or authors.
 */

package org.springframework.kafka.listener;

import org.jspecify.annotations.Nullable;

import org.springframework.context.ApplicationContext;
import org.springframework.util.Assert;

/**
 * A factory for {@link ContainerPartitionPausingBackOffManager}.
 *
 * @author Gary Russell
 * @since 2.9
 *
 */
public class ContainerPartitionPausingBackOffManagerFactory extends AbstractKafkaBackOffManagerFactory {

	private @Nullable BackOffHandler backOffHandler;

	/**
	 * Construct an instance with the provided properties.
	 * @param listenerContainerRegistry the registry.
	 * @param applicationContext the application context.
	 */
	@SuppressWarnings("this-escape")
	public ContainerPartitionPausingBackOffManagerFactory(@Nullable ListenerContainerRegistry listenerContainerRegistry,
			ApplicationContext applicationContext) {

		super(listenerContainerRegistry);
		setApplicationContext(applicationContext);
	}

	@Override
	protected KafkaConsumerBackoffManager doCreateManager(ListenerContainerRegistry registry) {
		Assert.notNull(this.backOffHandler, "a BackOffHandler is required");
		return new ContainerPartitionPausingBackOffManager(getListenerContainerRegistry(), this.backOffHandler);
	}

	/**
	 * Set the back off handler to use in the created handlers.
	 * @param backOffHandler the handler.
	 */
	public void setBackOffHandler(BackOffHandler backOffHandler) {
		this.backOffHandler = backOffHandler;
	}

}
