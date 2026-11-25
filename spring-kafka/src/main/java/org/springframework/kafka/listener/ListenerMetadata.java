/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
 */

package org.springframework.kafka.listener;

import org.jspecify.annotations.Nullable;

/**
 * Metadata associated to a {@link org.springframework.kafka.annotation.KafkaListener}.
 *
 * @author Francois Rosiere
 * @author Gary Russell
 * @author Ngoc Nhan
 * @since 2.8.5
 * @see org.springframework.kafka.annotation.KafkaListener
 */
public interface ListenerMetadata {

	/**
	 * Return the listener id.
	 * @return the listener id.
	 */
	@Nullable
	String getListenerId();

	/**
	 * Return the group id.
	 * @return the group id.
	 */
	@Nullable
	String getGroupId();

	/**
	 * Return the listener info.
	 * @return the listener info.
	 */
	byte @Nullable [] getListenerInfo();

}
