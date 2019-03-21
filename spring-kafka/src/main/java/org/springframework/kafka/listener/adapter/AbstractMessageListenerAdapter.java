/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.kafka.listener.adapter;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.TopicPartition;

import org.springframework.kafka.listener.ConsumerSeekAware;

/**
 * Top level class for all listener adapters.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @param <T> the delegate type.
 *
 * @author Gary Russell
 * @since 1.1
 *
 */
public abstract class AbstractMessageListenerAdapter<K, V, T> implements ConsumerSeekAware {

	protected final Log logger = LogFactory.getLog(this.getClass()); // NOSONAR

	protected final T delegate; //NOSONAR

	private final ConsumerSeekAware seekAware;

	public AbstractMessageListenerAdapter(T delegate) {
		this.delegate = delegate;
		if (delegate instanceof ConsumerSeekAware) {
			this.seekAware = (ConsumerSeekAware) delegate;
		}
		else {
			this.seekAware = null;
		}
	}

	@Override
	public void registerSeekCallback(ConsumerSeekCallback callback) {
		if (this.seekAware != null) {
			this.seekAware.registerSeekCallback(callback);
		}
	}

	@Override
	public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
		if (this.seekAware != null) {
			this.seekAware.onPartitionsAssigned(assignments, callback);
		}
	}

	@Override
	public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
		if (this.seekAware != null) {
			this.seekAware.onIdleContainer(assignments, callback);
		}
	}

}
