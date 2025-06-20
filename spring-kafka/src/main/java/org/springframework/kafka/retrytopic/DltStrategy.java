/*
 * Copyright 2018-present the original author or authors.
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

package org.springframework.kafka.retrytopic;

/**
 *
 * Strategies for handling DLT processing.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public enum DltStrategy {

	/**
	 * Don't create a DLT.
	 */
	NO_DLT,

	/**
	 * Always send the message back to the DLT for reprocessing in case of failure in
	 * DLT processing.
	 */
	ALWAYS_RETRY_ON_ERROR,

	/**
	 * Fail if DLT processing throws an error.
	 */
	FAIL_ON_ERROR

}
