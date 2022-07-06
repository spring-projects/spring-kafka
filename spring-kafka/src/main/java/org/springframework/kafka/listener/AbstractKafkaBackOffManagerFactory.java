/*
 * Copyright 2018-2022 the original author or authors.
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

package org.springframework.kafka.listener;

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

	private ApplicationContext applicationContext;

	private ListenerContainerRegistry listenerContainerRegistry;

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
	public AbstractKafkaBackOffManagerFactory(ListenerContainerRegistry listenerContainerRegistry) {
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
		return this.applicationContext.getBean(beanName, beanClass);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

}
