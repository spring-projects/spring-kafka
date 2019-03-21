/*
 * Copyright 2015-2016 the original author or authors.
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

package org.springframework.kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Handles errors thrown during the execution of a {@link BatchMessageListener}.
 * The listener should communicate which position(s) in the list failed in the
 * exception.
 *
 * @author Gary Russell
 *
 * @since 1.1
 */
public interface BatchErrorHandler extends GenericErrorHandler<ConsumerRecords<?, ?>> {

}
