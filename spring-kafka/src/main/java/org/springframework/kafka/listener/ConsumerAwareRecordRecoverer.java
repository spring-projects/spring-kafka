/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jspecify.annotations.Nullable;

/**
 * A {@link ConsumerRecordRecoverer} that supports getting a reference to the
 * {@link Consumer}.
 *
 * @author Gary Russell
 * @since 2.7
 *
 */
@FunctionalInterface
public interface ConsumerAwareRecordRecoverer extends ConsumerRecordRecoverer {

	@Override
	default void accept(ConsumerRecord<?, ?> record, Exception exception) {
		accept(record, null, exception);
	}

	/**
	 * Recover the record.
	 * @param record the record.
	 * @param consumer the consumer.
	 * @param exception the exception.
	 * @since 2.7
	 */
	void accept(ConsumerRecord<?, ?> record, @Nullable Consumer<?, ?> consumer, @Nullable Exception exception);

}
