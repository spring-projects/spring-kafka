/*
 * Copyright 2015-2016 the original author or authors.
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
 * Handle for acknowledging the processing of a
 * {@link org.apache.kafka.clients.consumer.ConsumerRecord}. Recipients can store the
 * reference in asynchronous scenarios, but the internal state should be assumed transient
 * (i.e. it cannot be serialized and deserialized later)
 *
 * @author Marius Bogoevici
 * @author Gary Russell
 */
public interface Acknowledgment {

	/**
	 * Invoked when the message for which the acknowledgment has been created has been processed.
	 * Calling this method implies that all the previous messages in the partition have been processed already.
	 */
	void acknowledge();

}
