/*
 * Copyright 2018 the original author or authors.
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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.messaging.MessageHeaders;
import org.springframework.util.Assert;
import org.springframework.util.PatternMatchUtils;

/**
 * Base for Kafka header mappers.
 *
 * @author Gary Russell
 * @since 2.1.3
 *
 */
public abstract class AbstractKafkaHeaderMapper implements KafkaHeaderMapper {

	protected final Log logger = LogFactory.getLog(getClass()); // NOSONAR

	private static final List<SimplePatternBasedHeaderMatcher> NEVER_MAPPED = Arrays.asList(
			new SimplePatternBasedHeaderMatcher("!" + KafkaHeaders.ACKNOWLEDGMENT),
			new SimplePatternBasedHeaderMatcher("!" + KafkaHeaders.CONSUMER),
			new SimplePatternBasedHeaderMatcher("!" + KafkaHeaders.MESSAGE_KEY),
			new SimplePatternBasedHeaderMatcher("!" + KafkaHeaders.OFFSET),
			new SimplePatternBasedHeaderMatcher("!" + KafkaHeaders.PARTITION_ID),
			new SimplePatternBasedHeaderMatcher("!" + KafkaHeaders.RAW_DATA),
			new SimplePatternBasedHeaderMatcher("!" + KafkaHeaders.RECEIVED_MESSAGE_KEY),
			new SimplePatternBasedHeaderMatcher("!" + KafkaHeaders.RECEIVED_PARTITION_ID),
			new SimplePatternBasedHeaderMatcher("!" + KafkaHeaders.RECEIVED_TIMESTAMP),
			new SimplePatternBasedHeaderMatcher("!" + KafkaHeaders.RECEIVED_TOPIC),
			new SimplePatternBasedHeaderMatcher("!" + KafkaHeaders.TIMESTAMP),
			new SimplePatternBasedHeaderMatcher("!" + KafkaHeaders.TIMESTAMP_TYPE),
			new SimplePatternBasedHeaderMatcher("!" + KafkaHeaders.BATCH_CONVERTED_HEADERS),
			new SimplePatternBasedHeaderMatcher("!" + KafkaHeaders.NATIVE_HEADERS),
			new SimplePatternBasedHeaderMatcher("!" + KafkaHeaders.TOPIC));

	protected final List<SimplePatternBasedHeaderMatcher> matchers = new ArrayList<>(NEVER_MAPPED); // NOSONAR

	public AbstractKafkaHeaderMapper(String... patterns) {
		Assert.notNull(patterns, "'patterns' must not be null");
		for (String pattern : patterns) {
			this.matchers.add(new SimplePatternBasedHeaderMatcher(pattern));
		}
	}

	protected boolean matches(String header, Object value) {
		if (matches(header)) {
			if ((header.equals(MessageHeaders.REPLY_CHANNEL) || header.equals(MessageHeaders.ERROR_CHANNEL))
					&& !(value instanceof String)) {
				if (this.logger.isDebugEnabled()) {
					this.logger.debug("Cannot map " + header + " when type is [" + value.getClass()
							+ "]; it must be a String");
				}
				return false;
			}
			return true;
		}
		return false;
	}

	protected boolean matches(String header) {
		for (SimplePatternBasedHeaderMatcher matcher : this.matchers) {
			if (matcher.matchHeader(header)) {
				return !matcher.isNegated();
			}
		}
		if (this.logger.isDebugEnabled()) {
			this.logger.debug(MessageFormat.format("headerName=[{0}] WILL NOT be mapped; matched no patterns",
					header));
		}
		return false;
	}

	/**
	 * A pattern-based header matcher that matches if the specified
	 * header matches the specified simple pattern.
	 * <p> The {@code negate == true} state indicates if the matching should be treated as "not matched".
	 * @see org.springframework.util.PatternMatchUtils#simpleMatch(String, String)
	 */
	protected static class SimplePatternBasedHeaderMatcher {

		private static final Log logger = LogFactory.getLog(SimplePatternBasedHeaderMatcher.class); // NOSONAR

		private final String pattern;

		private final boolean negate;

		public SimplePatternBasedHeaderMatcher(String pattern) {
			this(pattern.startsWith("!") ? pattern.substring(1) : pattern, pattern.startsWith("!"));
		}

		SimplePatternBasedHeaderMatcher(String pattern, boolean negate) {
			Assert.notNull(pattern, "Pattern must no be null");
			this.pattern = pattern.toLowerCase();
			this.negate = negate;
		}

		public boolean matchHeader(String headerName) {
			String header = headerName.toLowerCase();
			if (PatternMatchUtils.simpleMatch(this.pattern, header)) {
				if (logger.isDebugEnabled()) {
					logger.debug(
							MessageFormat.format(
									"headerName=[{0}] WILL " + (this.negate ? "NOT " : "")
											+ "be mapped, matched pattern=" + (this.negate ? "!" : "") + "{1}",
									headerName, this.pattern));
				}
				return true;
			}
			return false;
		}

		public boolean isNegated() {
			return this.negate;
		}

	}

}
