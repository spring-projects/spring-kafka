/*
 * Copyright 2017-2022 the original author or authors.
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

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Default implementation for {@link ListenerMetadata}.
 * @author Francois Rosiere
 * @since 2.8.6
 */
class DefaultListenerMetadata implements ListenerMetadata {

	private final MessageListenerContainer container;

	DefaultListenerMetadata(MessageListenerContainer container) {
		Assert.notNull(container, "'container' must not be null");
		this.container = container;
	}

	@Override
	@Nullable
	public String getListenerId() {
		return this.container.getListenerId();
	}

	@Override
	@Nullable
	public String getGroupId() {
		return this.container.getGroupId();
	}

	@Override
	@Nullable
	public byte[] getListenerInfo() {
		return this.container.getListenerInfo();
	}
}
