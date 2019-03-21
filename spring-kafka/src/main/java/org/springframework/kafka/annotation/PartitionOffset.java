/*
 * Copyright 2016 the original author or authors.
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

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used to add partition/initial offset information to a {@code KafkaListener}.
 *
 * @author Artem Bilan
 */
@Target({})
@Retention(RetentionPolicy.RUNTIME)
public @interface PartitionOffset {

	/**
	 * The partition within the topic to listen on.
	 * Property place holders and SpEL expressions are supported,
	 * which must resolve to Integer (or String that can be parsed as Integer).
	 * @return partition within the topic.
	 */
	String partition();

	/**
	 * The initial offset of the {@link #partition()}.
	 * Property place holders and SpEL expressions are supported,
	 * which must resolve to Long (or String that can be parsed as Long).
	 * @return initial offset.
	 */
	String initialOffset();

}
