/*
 * Copyright 2016 the original author or authors.
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

import org.springframework.kafka.listener.ConsumerSeekAware;

/**
 * Top level class for all listener adapters.
 *
 * @param <K> the key type.
 * @param <V> the value type.
 * @param <T> the delegate type.
 *
 * @author Gary Russell
 * @since 5.0
 *
 */
public abstract class AbstractMessageListenerAdapter<K, V, T> implements ConsumerSeekAware {

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

}
