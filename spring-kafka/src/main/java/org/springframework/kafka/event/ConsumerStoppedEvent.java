/*
 * Copyright 2018 the original author or authors.
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

/**
 * An event published when a consumer is stopped. While it is best practice to use
 * stateless listeners, you can consume this event to clean up any thread-based resources
 * (remove ThreadLocals, destroy thread-scoped beans etc), as long as the context event
 * multicaster is not modified to use an async task executor.
 *
 * @author Gary Russell
 * @since 2.2
 *
 */
@SuppressWarnings("serial")
public class ConsumerStoppedEvent extends KafkaEvent {

	/**
	 * Construct an instance with the provided source and partitions.
	 * @param source the container.
	 */
	public ConsumerStoppedEvent(Object source) {
		super(source);
	}

	@Override
	public String toString() {
		return "ConsumerStoppedEvent [source=" + getSource() + "]";
	}

}
