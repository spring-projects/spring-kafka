/*
 * Copyright 2014-2021 the original author or authors.
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

package org.springframework.kafka.annotation;

import org.springframework.util.MimeTypeUtils;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used to define the content type of {@code Message} with {@code ResolvableTypeResolver}.
 *
 * @author Scruel Tao
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.PARAMETER})
public @interface ResolvableType {
	/**
	 * The string value of the content type of message, which can support parse,
	 * Default application/json.
	 *
	 * @return the string value of mime type.
	 * @see org.springframework.util.MimeType
	 */
	String type() default MimeTypeUtils.APPLICATION_JSON_VALUE;

	/**
	 * Regardless of whether there is a type header in the {@code Message}, the parse
	 * progress will set the header of {@code Message} by this annotation {@link #type()}.
	 *
	 * @return whether to force parse with the annotation {@link #type()}
	 */
	boolean force() default false;
}
