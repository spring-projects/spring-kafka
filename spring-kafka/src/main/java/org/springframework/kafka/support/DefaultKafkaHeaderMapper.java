/*
 * Copyright 2017 the original author or authors.
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

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import org.springframework.messaging.MessageHeaders;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.MimeType;
import org.springframework.util.PatternMatchUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * Default header mapper for Apache Kafka.
 * Headers in {@link KafkaHeaders} are never mapped on outbound messages.
 *
 * @author Gary Russell
 * @since 1.3
 *
 */
public class DefaultKafkaHeaderMapper implements KafkaHeaderMapper {

	private static final Log logger = LogFactory.getLog(DefaultKafkaHeaderMapper.class);

	private static final List<String> DEFAULT_TRUSTED_PACKAGES =
			Arrays.asList(
					"java.util",
					"java.lang",
					"org.springframework.util"
			);

	/**
	 * Header name for java types of other headers.
	 */
	public static final String JSON_TYPES = "spring_json_header_types";

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

	private final ObjectMapper objectMapper;

	private final List<SimplePatternBasedHeaderMatcher> matchers = new ArrayList<>(NEVER_MAPPED);

	private final Set<String> trustedPackages = new LinkedHashSet<>(DEFAULT_TRUSTED_PACKAGES);

	/**
	 * Construct an instance with the default object mapper and default header patterns
	 * for outbound headers; all inbound headers are mapped. The default pattern list is
	 * {@code "!id", "!timestamp" and "*"}. In addition, none of the headers in
	 * {@link KafkaHeaders} are ever mapped as headers since they represent data in
	 * consumer/producer records.
	 * @see #DefaultKafkaHeaderMapper(ObjectMapper)
	 */
	public DefaultKafkaHeaderMapper() {
		this(new ObjectMapper());
	}

	/**
	 * Construct an instance with the provided object mapper and default header patterns
	 * for outbound headers; all inbound headers are mapped. The patterns are applied in
	 * order, stopping on the first match (positive or negative). Patterns are negated by
	 * preceding them with "!". The default pattern list is
	 * {@code "!id", "!timestamp" and "*"}. In addition, none of the headers in
	 * {@link KafkaHeaders} are ever mapped as headers since they represent data in
	 * consumer/producer records.
	 * @param objectMapper the object mapper.
	 * @see org.springframework.util.PatternMatchUtils#simpleMatch(String, String)
	 */
	public DefaultKafkaHeaderMapper(ObjectMapper objectMapper) {
		this(objectMapper,
				"!" + MessageHeaders.ID,
				"!" + MessageHeaders.TIMESTAMP,
				"*");
	}

	/**
	 * Construct an instance with a default object mapper and the provided header patterns
	 * for outbound headers; all inbound headers are mapped. The patterns are applied in
	 * order, stopping on the first match (positive or negative). Patterns are negated by
	 * preceding them with "!". The patterns will replace the default patterns; you
	 * generally should not map the {@code "id" and "timestamp"} headers. Note:
	 * none of the headers in {@link KafkaHeaders} are ever mapped as headers since they
	 * represent data in consumer/producer records.
	 * @param patterns the patterns.
	 * @see org.springframework.util.PatternMatchUtils#simpleMatch(String, String)
	 */
	public DefaultKafkaHeaderMapper(String... patterns) {
		this(new ObjectMapper(), patterns);
	}

	/**
	 * Construct an instance with the provided object mapper and the provided header
	 * patterns for outbound headers; all inbound headers are mapped. The patterns are
	 * applied in order, stopping on the first match (positive or negative). Patterns are
	 * negated by preceding them with "!". The patterns will replace the default patterns;
	 * you generally should not map the {@code "id" and "timestamp"} headers. Note: none
	 * of the headers in {@link KafkaHeaders} are ever mapped as headers since they
	 * represent data in consumer/producer records.
	 * @param objectMapper the object mapper.
	 * @param patterns the patterns.
	 * @see org.springframework.util.PatternMatchUtils#simpleMatch(String, String)
	 */
	public DefaultKafkaHeaderMapper(ObjectMapper objectMapper, String... patterns) {
		Assert.notNull(objectMapper, "'objectMapper' must not be null");
		Assert.notNull(patterns, "'patterns' must not be null");
		Assert.noNullElements(patterns, "'patterns' must not have null elements");
		this.objectMapper = objectMapper;
		for (String pattern : patterns) {
			this.matchers.add(new SimplePatternBasedHeaderMatcher(pattern));
		}
		Module module = new SimpleModule().addDeserializer(MimeType.class, new MimeTypeJsonDeserializer(objectMapper));
		this.objectMapper.registerModule(module);
	}

	/**
	 * Return the object mapper.
	 * @return the mapper.
	 */
	protected ObjectMapper getObjectMapper() {
		return this.objectMapper;
	}

	/**
	 * Add packages to the trusted packages list (default {@code java.util, java.lang}) used
	 * when constructing objects from JSON.
	 * If any of the supplied packages is {@code "*"}, all packages are trusted.
	 * If a class for a non-trusted package is encountered, the header is returned to the
	 * application with value of type {@link NonTrustedHeaderType}.
	 * @param trustedPackages the packages to trust.
	 */
	public void addTrustedPackages(String... trustedPackages) {
		if (trustedPackages != null) {
			for (String whiteList : trustedPackages) {
				if ("*".equals(whiteList)) {
					this.trustedPackages.clear();
					break;
				}
				else {
					this.trustedPackages.add(whiteList);
				}
			}
		}
	}

	@Override
	public void fromHeaders(MessageHeaders headers, Headers target) {
		final Map<String, String> jsonHeaders = new HashMap<>();
		headers.forEach((k, v) -> {
			if (matches(k)) {
				if (v instanceof byte[]) {
					target.add(new RecordHeader(k, (byte[]) v));
				}
				else {
					try {
						target.add(new RecordHeader(k, getObjectMapper().writeValueAsBytes(v)));
						jsonHeaders.put(k, v.getClass().getName());
					}
					catch (Exception e) {
						if (logger.isDebugEnabled()) {
							logger.debug("Could not map " + k + " with type " + v.getClass().getName());
						}
					}
				}
			}
		});
		if (jsonHeaders.size() > 0) {
			try {
				target.add(new RecordHeader(JSON_TYPES, getObjectMapper().writeValueAsBytes(jsonHeaders)));
			}
			catch (IllegalStateException | JsonProcessingException e) {
				logger.error("Could not add json types header", e);
			}
		}
	}

	protected boolean matches(String header) {
		for (SimplePatternBasedHeaderMatcher matcher : this.matchers) {
			if (matcher.matchHeader(header)) {
				return !matcher.isNegated();
			}
		}
		if (logger.isDebugEnabled()) {
			logger.debug(MessageFormat.format("headerName=[{0}] WILL NOT be mapped; matched no patterns", header));
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void toHeaders(Headers source, final Map<String, Object> headers) {
		Map<String, String> types = null;
		Iterator<Header> iterator = source.iterator();
		while (iterator.hasNext()) {
			Header next = iterator.next();
			if (next.key().equals(JSON_TYPES)) {
				try {
					types = getObjectMapper().readValue(next.value(), HashMap.class);
				}
				catch (IOException e) {
					logger.error("Could not decode json types: " + new String(next.value()), e);
				}
				break;
			}
		}
		final Map<String, String> jsonTypes = types;
		source.forEach(h -> {
			if (!(h.key().equals(JSON_TYPES))) {
				if (jsonTypes != null && jsonTypes.containsKey(h.key())) {
					Class<?> type = Object.class;
					String requestedType = jsonTypes.get(h.key());
					boolean trusted = false;
					try {
						trusted = trusted(requestedType);
						if (trusted) {
							type = ClassUtils.forName(requestedType, null);
						}
					}
					catch (Exception e) {
						logger.error("Could not load class for header: " + h.key(), e);
					}
					if (trusted) {
						try {
							headers.put(h.key(), getObjectMapper().readValue(h.value(), type));
						}
						catch (IOException e) {
							logger.error("Could not decode json type: " + new String(h.value()) + " for key: " + h.key(),
									e);
							headers.put(h.key(), h.value());
						}
					}
					else {
						headers.put(h.key(), new NonTrustedHeaderType(h.value(), requestedType));
					}
				}
				else {
					headers.put(h.key(), h.value());
				}
			}
		});
	}

	protected boolean trusted(String requestedType) {
		if (!this.trustedPackages.isEmpty()) {
			int lastDot = requestedType.lastIndexOf(".");
			if (lastDot < 0) {
				return false;
			}
			String packageName = requestedType.substring(0, lastDot);
			for (String trustedPackage : this.trustedPackages) {
				if (packageName.equals(trustedPackage) || packageName.startsWith(trustedPackage + ".")) {
					return true;
				}
			}
			return false;
		}
		return true;
	}

	/**
	 * A pattern-based header matcher that matches if the specified
	 * header matches the specified simple pattern.
	 * <p> The {@code negate == true} state indicates if the matching should be treated as "not matched".
	 * @see org.springframework.util.PatternMatchUtils#simpleMatch(String, String)
	 */
	protected static class SimplePatternBasedHeaderMatcher {

		private static final Log logger = LogFactory.getLog(SimplePatternBasedHeaderMatcher.class);

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

	/**
	 * Represents a header that could not be decoded due to an untrusted type.
	 */
	public static class NonTrustedHeaderType {

		private final byte[] headerValue;

		private final String untrustedType;

		NonTrustedHeaderType(byte[] headerValue, String untrustedType) {
			this.headerValue = headerValue;
			this.untrustedType = untrustedType;
		}

		public byte[] getHeaderValue() {
			return this.headerValue;
		}

		public String getUntrustedType() {
			return this.untrustedType;
		}

		@Override
		public String toString() {
			return "NonTrustedHeaderType [headerValue=" + Arrays.toString(this.headerValue) + ", untrustedType="
					+ this.untrustedType + "]";
		}

	}

}
