/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.kafka.listener;

import java.util.Objects;

import org.jspecify.annotations.Nullable;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.kafka.config.KafkaListenerConfigUtils;
import org.springframework.util.Assert;

/**
 * Base class for {@link KafkaBackOffManagerFactory} implementations.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.7
 * @see KafkaConsumerBackoffManager
 */
public abstract class AbstractKafkaBackOffManagerFactory
		implements KafkaBackOffManagerFactory, ApplicationContextAware {

	private @Nullable ApplicationContext applicationContext;

	private @Nullable ListenerContainerRegistry listenerContainerRegistry;

	/**
	 * Creates an instance that will retrieve the {@link ListenerContainerRegistry} from
	 * the {@link ApplicationContext}.
	 */
	public AbstractKafkaBackOffManagerFactory() {
		this.listenerContainerRegistry = null;
	}

	/**
	 * Creates an instance with the provided {@link ListenerContainerRegistry},
	 * which will be used to fetch the {@link MessageListenerContainer} to back off.
	 * @param listenerContainerRegistry the listenerContainerRegistry to use.
	 */
	public AbstractKafkaBackOffManagerFactory(@Nullable ListenerContainerRegistry listenerContainerRegistry) {
		this.listenerContainerRegistry = listenerContainerRegistry;
	}

	/**
	 * Sets the {@link ListenerContainerRegistry}, that will be used to fetch the
	 * {@link MessageListenerContainer} to back off.
	 *
	 * @param listenerContainerRegistry the listenerContainerRegistry to use.
	 */
	public void setListenerContainerRegistry(ListenerContainerRegistry listenerContainerRegistry) {
		this.listenerContainerRegistry = listenerContainerRegistry;
	}

	@Override
	public KafkaConsumerBackoffManager create() {
		return doCreateManager(getListenerContainerRegistry());
	}

	protected abstract KafkaConsumerBackoffManager doCreateManager(ListenerContainerRegistry registry);

	protected ListenerContainerRegistry getListenerContainerRegistry() {
		return this.listenerContainerRegistry != null
				? this.listenerContainerRegistry
				: getListenerContainerFromContext();
	}

	private ListenerContainerRegistry getListenerContainerFromContext() {
		Assert.notNull(this.applicationContext, "ApplicationContext not set.");
		return this.applicationContext.getBean(KafkaListenerConfigUtils.KAFKA_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME,
				ListenerContainerRegistry.class);
	}

	protected <T> T getBean(String beanName, Class<T> beanClass) {
		return Objects.requireNonNull(this.applicationContext).getBean(beanName, beanClass);
	}

	@Override
	public final void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

}
