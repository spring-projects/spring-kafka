/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.kafka.listener.adapter;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.TopicPartition;

import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.listener.DelegatingMessageListener;
import org.springframework.kafka.listener.ListenerType;
import org.springframework.kafka.listener.ListenerUtils;

/**
 * Top level class for all listener adapters.
 *
 * @param <T> the delegate type.
 *
 * @author Gary Russell
 * @since 1.1
 *
 */
public abstract class AbstractDelegatingMessageListenerAdapter<T>
		implements ConsumerSeekAware, DelegatingMessageListener<T> {

	protected final Log logger = LogFactory.getLog(this.getClass()); // NOSONAR

	protected final T delegate; //NOSONAR

	protected final ListenerType delegateType; // NOSONAR

	private final ConsumerSeekAware seekAware;

	public AbstractDelegatingMessageListenerAdapter(T delegate) {
		this.delegate = delegate;
		this.delegateType = ListenerUtils.determineListenerType(delegate);
		if (delegate instanceof ConsumerSeekAware) {
			this.seekAware = (ConsumerSeekAware) delegate;
		}
		else {
			this.seekAware = null;
		}
	}

	@Override
	public T getDelegate() {
		return this.delegate;
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
