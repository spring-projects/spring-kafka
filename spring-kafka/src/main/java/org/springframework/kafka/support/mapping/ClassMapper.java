/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.kafka.support.mapping;

import org.apache.kafka.common.header.Headers;
import org.jspecify.annotations.Nullable;

/**
 * Strategy for setting metadata on messages such that one can create the class
 * that needs to be instantiated when receiving a message.
 *
 * @author Mark Pollack
 * @author James Carr
 * @author Gary Russell
 *
 * @since 2.1
 */
public interface ClassMapper {

	void fromClass(Class<?> clazz, Headers headers);

	@Nullable
	Class<?> toClass(Headers headers);

}
