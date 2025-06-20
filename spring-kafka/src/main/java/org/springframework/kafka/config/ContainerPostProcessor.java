/*
 * Copyright 2023-present the original author or authors.
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

package org.springframework.kafka.config;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;

/**
 * Called by the container factory after the container is created and configured. This
 * post processor is applied on the listener through the
 * {@link KafkaListener#containerPostProcessor()} attribute.
 * <p>
 * A {@link ContainerCustomizer} can be used when customization must be applied to all
 * containers. In that case, this will be applied after the customizer.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @param <C> the container type.
 *
 * @author Francois Rosiere
 * @since 3.1
 *
 * @see ContainerCustomizer
 * @see KafkaListener
 */
@FunctionalInterface
public interface ContainerPostProcessor<K, V, C extends AbstractMessageListenerContainer<K, V>> {

	/**
	 * Post processes the container.
	 *
	 * @param container the container.
	 */
	void postProcess(C container);

}
