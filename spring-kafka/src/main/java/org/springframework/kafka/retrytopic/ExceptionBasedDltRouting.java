/*
 * Copyright 2023 the original author or authors.
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
 * Annotation allowing to specify the custom DLT routing rules steered by exceptions
 * which might be thrown during the processing.
 *
 * @author Adrian Chlebosz
 * @see ExceptionBasedDltDestination
 * @since 3.2.0
 */
public @interface ExceptionBasedDltRouting {

	/**
	 * Specific rules expressing to which custom DLT the message should be redirected
	 * when the specified exception has been thrown during its processing.
	 *
	 * @return configured routing
	 */
	ExceptionBasedDltDestination[] routingRules() default {};
}
