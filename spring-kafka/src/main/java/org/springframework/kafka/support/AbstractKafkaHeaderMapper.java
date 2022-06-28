/*
 * Copyright 2018-2022 the original author or authors.
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

package org.springframework.kafka.support;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.header.Header;

import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.PatternMatchUtils;

/**
 * Base for Kafka header mappers.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.1.3
 *
 */
public abstract class AbstractKafkaHeaderMapper implements KafkaHeaderMapper {

	protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass())); // NOSONAR

	private final List<HeaderMatcher> matchers = new ArrayList<>();

	private final Map<String, Boolean> rawMappedHeaders = new HashMap<>();

	{
		this.rawMappedHeaders.put(KafkaHeaders.LISTENER_INFO, true);
	}

	private final boolean outbound;

	private boolean mapAllStringsOut;

	private Charset charset = StandardCharsets.UTF_8;

	/**
	 * Construct a mapper that will match the supplied patterns (outbound) and all headers
	 * (inbound). For outbound mapping, certain internal framework headers are never
	 * mapped.
	 * @param patterns the patterns.
	 */
	public AbstractKafkaHeaderMapper(String... patterns) {
		this(true, patterns);
	}

	/**
	 * Construct a mapper that will match the supplied patterns (outbound) and all headers
	 * (inbound). For outbound mapping, certain internal framework headers are never
	 * mapped.
	 * @param outbound true for an outbound mapper.
	 * @param patterns the patterns.
	 */
	protected AbstractKafkaHeaderMapper(boolean outbound, String... patterns) {
		Assert.notNull(patterns, "'patterns' must not be null");
		this.outbound = outbound;
		if (outbound) {
			this.matchers.add(new NeverMatchHeaderMatcher(
					KafkaHeaders.ACKNOWLEDGMENT,
					KafkaHeaders.CONSUMER,
					KafkaHeaders.KEY,
					KafkaHeaders.OFFSET,
					KafkaHeaders.PARTITION,
					KafkaHeaders.RAW_DATA,
					KafkaHeaders.RECEIVED_KEY,
					KafkaHeaders.RECEIVED_PARTITION,
					KafkaHeaders.RECEIVED_TIMESTAMP,
					KafkaHeaders.RECEIVED_TOPIC,
					KafkaHeaders.TIMESTAMP,
					KafkaHeaders.TIMESTAMP_TYPE,
					KafkaHeaders.BATCH_CONVERTED_HEADERS,
					KafkaHeaders.NATIVE_HEADERS,
					KafkaHeaders.TOPIC,
					KafkaHeaders.DELIVERY_ATTEMPT,
					KafkaHeaders.LISTENER_INFO,
					KafkaHeaders.GROUP_ID));
		}
		for (String pattern : patterns) {
			this.matchers.add(new SimplePatternBasedHeaderMatcher(pattern));
		}
	}

	/**
	 * Subclasses can invoke this to add custom {@link HeaderMatcher}s.
	 * @param matchersToAdd the matchers to add.
	 * @since 2.3
	 */
	protected final void addMatchers(HeaderMatcher... matchersToAdd) {
		Assert.notNull(matchersToAdd, "'matchersToAdd' cannot be null");
		Assert.noNullElements(matchersToAdd, "'matchersToAdd' cannot have null elements");
		Collections.addAll(this.matchers, matchersToAdd);
	}

	/**
	 * Set to true to map all {@code String} valued outbound headers to {@code byte[]}.
	 * To map to a {@code String} for inbound, there must be an entry in the rawMappedHeaders map.
	 * @param mapAllStringsOut true to map all strings.
	 * @since 2.2.5
	 * @see #setRawMappedHeaders(Map)
	 */
	public void setMapAllStringsOut(boolean mapAllStringsOut) {
		this.mapAllStringsOut = mapAllStringsOut;
	}

	protected Charset getCharset() {
		return this.charset;
	}

	/**
	 * Set the charset to use when mapping String-valued headers to/from byte[]. Default UTF-8.
	 * @param charset the charset.
	 * @since 2.2.5
	 * @see #setRawMappedHeaders(Map)
	 */
	public void setCharset(Charset charset) {
		Assert.notNull(charset, "'charset' cannot be null");
		this.charset = charset;
	}

	/**
	 * Set the headers to not perform any conversion on (except {@code String} to
	 * {@code byte[]} for outbound). Inbound headers that match will be mapped as
	 * {@code byte[]} unless the corresponding boolean in the map value is true,
	 * in which case it will be mapped as a String.
	 * @param rawMappedHeaders the header names to not convert and
	 * @since 2.2.5
	 * @see #setCharset(Charset)
	 * @see #setMapAllStringsOut(boolean)
	 */
	public void setRawMappedHeaders(Map<String, Boolean> rawMappedHeaders) {
		if (!ObjectUtils.isEmpty(rawMappedHeaders)) {
			this.rawMappedHeaders.clear();
			this.rawMappedHeaders.putAll(rawMappedHeaders);
		}
	}

	/**
	 * Add a raw mapped header.
	 * @param name the header name.
	 * @param toString convert to string on inbound when true.
	 * @since 2.7.1
	 * @see #setRawMappedHeaders(Map)
	 */
	public void addRawMappedHeader(String name, boolean toString) {
		this.rawMappedHeaders.put(name, toString);
	}

	protected boolean matches(String header, Object value) {
		if (matches(header)) {
			if ((header.equals(MessageHeaders.REPLY_CHANNEL) || header.equals(MessageHeaders.ERROR_CHANNEL))
					&& !(value instanceof String)) {
				this.logger.debug(() -> "Cannot map " + header + " when type is [" + value.getClass()
						+ "]; it must be a String");
				return false;
			}
			return true;
		}
		return false;
	}

	protected boolean matches(String header) {
		Assert.state(this.outbound, "This mapper cannot be used for outbound mapping");
		return doesMatch(header);
	}

	/**
	 * Matches header names for inbound mapping when configured as an inbound mapper.
	 * @param header the header name.
	 * @return true if it can be mapped.
	 * @since 2.8.8
	 */
	protected boolean matchesForInbound(String header) {
		if (this.outbound) {
			return true;
		}
		if (this.matchers.size() == 0) {
			return true;
		}
		return doesMatch(header);
	}

	private boolean doesMatch(String header) {
		for (HeaderMatcher matcher : this.matchers) {
			if (matcher.matchHeader(header)) {
				return !matcher.isNegated();
			}
		}
		this.logger.debug(() -> MessageFormat.format("headerName=[{0}] WILL NOT be mapped; matched no patterns",
				header));
		return false;
	}

	/**
	 * Check if the value is a String and convert to byte[], if so configured.
	 * @param key the header name.
	 * @param value the header value.
	 * @return the value to add.
	 * @since 2.2.5
	 */
	protected Object headerValueToAddOut(String key, Object value) {
		Object valueToAdd = mapRawOut(key, value);
		if (valueToAdd == null) {
			valueToAdd = value;
		}
		return valueToAdd;
	}

	@Nullable
	private byte[] mapRawOut(String header, Object value) {
		if (this.mapAllStringsOut || this.rawMappedHeaders.containsKey(header)) {
			if (value instanceof byte[]) {
				return (byte[]) value;
			}
			else if (value instanceof String) {
				return ((String) value).getBytes(this.charset);
			}
		}
		return null;
	}

	/**
	 * Check if the header value should be mapped to a String, if so configured.
	 * @param header the header.
	 * @return the value to add.
	 */
	protected Object headerValueToAddIn(Header header) {
		Object mapped = mapRawIn(header.key(), header.value());
		if (mapped == null) {
			mapped = header.value();
		}
		return mapped;
	}

	@Nullable
	private String mapRawIn(String header, byte[] value) {
		Boolean asString = this.rawMappedHeaders.get(header);
		if (Boolean.TRUE.equals(asString)) {
			return new String(value, this.charset);
		}
		return null;
	}


	/**
	 * A matcher for headers.
	 * @since 2.3
	 */
	protected interface HeaderMatcher {

		/**
		 * Return true if the header matches.
		 * @param headerName the header name.
		 * @return true for a match.
		 */
		boolean matchHeader(String headerName);

		/**
		 * Return true if this matcher is a negative matcher.
		 * @return true for a negative matcher.
		 */
		boolean isNegated();

	}

	/**
	 * A matcher that never matches a set of headers.
	 * @since 2.3
	 */
	protected static class NeverMatchHeaderMatcher implements HeaderMatcher {

		private final Set<String> neverMatchHeaders;

		protected NeverMatchHeaderMatcher(String... headers) {
			this.neverMatchHeaders = Arrays.stream(headers)
					.collect(Collectors.toSet());
		}

		@Override
		public boolean matchHeader(String headerName) {
			return this.neverMatchHeaders.contains(headerName);
		}

		@Override
		public boolean isNegated() {
			return true;
		}

	}

	/**
	 * A pattern-based header matcher that matches if the specified
	 * header matches the specified simple pattern.
	 * <p> The {@code negate == true} state indicates if the matching should be treated as "not matched".
	 * @see org.springframework.util.PatternMatchUtils#simpleMatch(String, String)
	 */
	protected static class SimplePatternBasedHeaderMatcher implements HeaderMatcher {

		private static final LogAccessor LOGGER =
				new LogAccessor(LogFactory.getLog(SimplePatternBasedHeaderMatcher.class));

		private final String pattern;

		private final boolean negate;

		protected SimplePatternBasedHeaderMatcher(String pattern) {
			this(pattern.startsWith("!") ? pattern.substring(1) : pattern, pattern.startsWith("!"));
		}

		SimplePatternBasedHeaderMatcher(String pattern, boolean negate) {
			Assert.notNull(pattern, "Pattern must no be null");
			this.pattern = pattern.toLowerCase();
			this.negate = negate;
		}

		@Override
		public boolean matchHeader(String headerName) {
			String header = headerName.toLowerCase();
			if (PatternMatchUtils.simpleMatch(this.pattern, header)) {
				LOGGER.debug(() ->
						MessageFormat.format(
								"headerName=[{0}] WILL " + (this.negate ? "NOT " : "")
										+ "be mapped, matched pattern=" + (this.negate ? "!" : "") + "{1}",
								headerName, this.pattern));
				return true;
			}
			return false;
		}

		@Override
		public boolean isNegated() {
			return this.negate;
		}

	}

}
