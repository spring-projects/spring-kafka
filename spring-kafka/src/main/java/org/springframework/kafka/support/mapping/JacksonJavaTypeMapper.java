/*
 * Copyright 2017-present the original author or authors.
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

package org.springframework.kafka.support.mapping;

import org.apache.kafka.common.header.Headers;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JavaType;

/**
 * Strategy for setting metadata on messages such that one can create the class that needs
 * to be instantiated when receiving a message. Basedon on Jackson 3.
 *
 * @author Mark Pollack
 * @author James Carr
 * @author Sam Nelson
 * @author Andreas Asplund
 * @author Gary Russell
 * @author Soby Chacko
 *
 * @since 4.0
 */
public interface JacksonJavaTypeMapper extends ClassMapper {

	/**
	 * The precedence for type conversion - inferred from the method parameter or message
	 * headers. Only applies if both exist.
	 */
	enum TypePrecedence {

		/**
		 * The type is inferred from the destination method.
		 */
		INFERRED,

		/**
		 * The type is obtained from headers.
		 */
		TYPE_ID
	}

	void fromJavaType(JavaType javaType, Headers headers);

	@Nullable
	JavaType toJavaType(Headers headers);

	TypePrecedence getTypePrecedence();

	/**
	 * Set the precedence for evaluating type information in message properties.
	 * When using {@code @KafkaListener} at the method level, the framework attempts
	 * to determine the target type for payload conversion from the method signature.
	 * If so, this type is provided by the {@code MessagingMessageListenerAdapter}.
	 * <p> By default, if the type is concrete (not abstract, not an interface), this will
	 * be used ahead of type information provided in the {@code __TypeId__} and
	 * associated headers provided by the sender.
	 * <p> If you wish to force the use of the  {@code __TypeId__} and associated headers
	 * (such as when the actual type is a subclass of the method argument type),
	 * set the precedence to {@link TypePrecedence#TYPE_ID}.
	 * @param typePrecedence the precedence.
	 */
	default void setTypePrecedence(TypePrecedence typePrecedence) {
		throw new UnsupportedOperationException("This mapper does not support this method");
	}

	void addTrustedPackages(String... packages);

	/**
	 * Remove the type information headers.
	 * @param headers the headers.
	 */
	default void removeHeaders(Headers headers) {
		// NOSONAR
	}

}
