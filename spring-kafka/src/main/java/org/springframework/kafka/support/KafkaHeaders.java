/*
 * Copyright 2014-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.support;

/**
 * @author Artem Bilan
 * @author Marius Bogoevici
 */
public abstract class KafkaHeaders {

	private static final String PREFIX = "kafka_";

	public static final String TOPIC = PREFIX + "topic";

	public static final String MESSAGE_KEY = PREFIX + "messageKey";

	public static final String PARTITION_ID = PREFIX + "partitionId";

	public static final String OFFSET = PREFIX + "offset";

	public static final String ACKNOWLEDGMENT = PREFIX + "acknowledgment";

}
