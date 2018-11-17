/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.event;

import org.springframework.context.ApplicationEvent;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;


/**
 * Base class for events.
 *
 * @author Gary Russell
 *
 */
public abstract class KafkaEvent extends ApplicationEvent {

	private static final long serialVersionUID = 1L;

	private final AbstractMessageListenerContainer<?, ?> container;

	@Deprecated
	public KafkaEvent(Object source) {
		this(source, null); // NOSONAR
	}

	public KafkaEvent(Object source, AbstractMessageListenerContainer<?, ?> container) {
		super(source);
		this.container = container;
	}

	public AbstractMessageListenerContainer<?, ?> getContainer() {
		return this.container;
	}

}
