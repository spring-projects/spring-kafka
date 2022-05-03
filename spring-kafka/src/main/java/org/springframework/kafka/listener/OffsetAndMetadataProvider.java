/*
 * Copyright 2016-2022 the original author or authors.
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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;

/**
 * Provider for {@link OffsetAndMetadata}. The provider can be used to have more granularity when creating an
 * {@link OffsetAndMetadata}. The provider is used for both sync and async commits of the offsets.
 *
 * @author Francois Rosiere
 * @since 2.8.5
 * @see org.apache.kafka.clients.consumer.OffsetCommitCallback
 */
public interface OffsetAndMetadataProvider {

	/**
	 * Provide an offset and metadata object for the given listener metadata and offset.
	 *
	 * @param listenerMetadata metadata associated to a listener.
	 * @param offset an offset.
	 * @return an offset and metadata.
	 */
	OffsetAndMetadata provide(ListenerMetadata listenerMetadata, long offset);
}
