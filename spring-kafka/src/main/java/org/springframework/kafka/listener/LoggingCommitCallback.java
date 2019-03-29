/*
 * Copyright 2019 the original author or authors.
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
