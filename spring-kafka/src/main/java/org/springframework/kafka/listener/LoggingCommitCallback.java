/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.kafka.listener;

import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import org.springframework.core.log.LogAccessor;

/**
 * Logs commit results at DEBUG level for success and ERROR for failures.
 *
 * @author Gary Russell
 * @since 2.2.4
 */
public final class LoggingCommitCallback implements OffsetCommitCallback {

	private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(LoggingCommitCallback.class));

	@Override
	public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
		if (exception != null) {
			LOGGER.error(exception, () -> "Commit failed for " + offsets);
		}
		else {
			LOGGER.debug(() -> "Commits for " + offsets + " completed");
		}
	}

}
