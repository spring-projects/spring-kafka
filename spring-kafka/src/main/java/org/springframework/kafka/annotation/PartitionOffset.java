/*
 * Copyright 2016-2020 the original author or authors.
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
 * @author Gary Russell
 */
@Target({})
@Retention(RetentionPolicy.RUNTIME)
public @interface PartitionOffset {

	/**
	 * Specifies the partition within the topic to listen on. Property placeholders and
	 * Spring Expression Language (SpEL) expressions are supported. These must resolve to
	 * an Integer or a String that can be parsed as an Integer. The '*' symbol indicates
	 * that the initial offset will be applied to all partitions within the specified
	 * {@link TopicPartition}. The string can also contain a comma-delimited list of
	 * individual partitions or ranges of partitions (e.g., {@code 0-5, 7, 10-15}). In this
	 * case, the offset will be applied to all specified partitions.
	 *
	 * @return The specified partition or partitions within the topic.
	 */
	String partition();

	/**
	 * Specifies the initial offset of the {@link #partition()}. Property placeholders and
	 * Spring Expression Language (SpEL) expressions are supported. These expressions must
	 * resolve to a Long or a String that can be parsed as a Long.
	 *
	 * @return The initial offset for the partition, indicating the starting point for
	 *         processing messages.
	 */
	String initialOffset();

	/**
	 * Determines whether the {@link #initialOffset()} is relative to the current
	 * consumer position. By default, a positive initial offset is treated as an absolute
	 * value, while a negative offset is relative to the current end of the topic. When
	 * this property is set to 'true', both positive and negative initial offsets are
	 * interpreted as relative to the current consumer position.
	 *
	 * @return True if the offset is relative to the current consumer position, false otherwise.
	 * @since 1.1
	 */
	String relativeToCurrent() default "false";

}
