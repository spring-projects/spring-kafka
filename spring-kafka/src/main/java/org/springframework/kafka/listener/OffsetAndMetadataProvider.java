/*
 * Copyright 2016-present the original author or authors.
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
