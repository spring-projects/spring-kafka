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
 * Used to add topic/partition information to a {@code KafkaListener}.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 */
@Target({})
@Retention(RetentionPolicy.RUNTIME)
public @interface TopicPartition {

	/**
	 * The topic to listen on.
	 * @return the topic to listen on. Property place holders
	 * and SpEL expressions are supported, which must resolve
	 * to a String.
	 */
	String topic();

	/**
	 * The partitions within the topic.
	 * Partitions specified here can't be duplicated in {@link #partitionOffsets()}.
	 * @return the partitions within the topic. Property place
	 * holders and SpEL expressions are supported, which must
	 * resolve to Integers (or Strings that can be parsed as
	 * Integers).
	 */
	String[] partitions() default {};

	/**
	 * The partitions with initial offsets within the topic.
	 * Partitions specified here can't be duplicated in the {@link #partitions()}.
	 * @return the {@link PartitionOffset} array.
	 */
	PartitionOffset[] partitionOffsets() default {};

}
