/*
 * Copyright 2016-present the original author or authors.
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
 * Annotation allowing to specify additional DLT which will be chosen when message
 * processing caused the configured exception to be thrown.
 *
 * @author Adrian Chlebosz
 *
 * @since 3.2.0
 */
public @interface ExceptionBasedDltDestination {

	/**
	 * Suffix extension used when constructing the name for the new DLT. It is placed
	 * before the main suffix configured through the
	 * ${@link org.springframework.kafka.annotation.RetryableTopic#dltTopicSuffix()}, so the
	 * final name is the product of these two.
	 * @return the configured suffix extension
	 */
	String suffix();

	/**
	 * When message processing throws one of the exceptions configured here, then
	 * it should be eventually redirected to the DLT with name containing the extension
	 * configured through {@link #suffix()}. The causes of the thrown exception will be
	 * traversed to match with any of configured ones.
	 * @return configured exceptions
	 */
	Class<? extends Throwable>[] exceptions();
}
