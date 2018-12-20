/*
 * Copyright 2017-2018 the original author or authors.
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;

import org.springframework.lang.Nullable;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.MimeType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdNodeBasedDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * Default header mapper for Apache Kafka.
 * Most headers in {@link KafkaHeaders} are not mapped on outbound messages.
 * The exceptions are correlation and reply headers for request/reply
 * messaging.
 * Header types are added to a special header {@link #JSON_TYPES}.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.3
 *
 */
public class DefaultKafkaHeaderMapper extends AbstractKafkaHeaderMapper {

	private static final List<String> DEFAULT_TRUSTED_PACKAGES =
			Arrays.asList(
					"java.util",
					"java.lang",
					"org.springframework.util"
			);

	private static final List<String> DEFAULT_TO_STRING_CLASSES =
			Arrays.asList(
					"org.springframework.util.MimeType",
					"org.springframework.http.MediaType"
			);

	/**
	 * Header name for java types of other headers.
	 */
	public static final String JSON_TYPES = "spring_json_header_types";

	private final ObjectMapper objectMapper;

	private final Set<String> trustedPackages = new LinkedHashSet<>(DEFAULT_TRUSTED_PACKAGES);

	private final Set<String> toStringClasses = new LinkedHashSet<>(DEFAULT_TO_STRING_CLASSES);

	/**
	 * Construct an instance with the default object mapper and default header patterns
	 * for outbound headers; all inbound headers are mapped. The default pattern list is
	 * {@code "!id", "!timestamp" and "*"}. In addition, most of the headers in
	 * {@link KafkaHeaders} are never mapped as headers since they represent data in
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
	 * {@code "!id", "!timestamp" and "*"}. In addition, most of the headers in
	 * {@link KafkaHeaders} are never mapped as headers since they represent data in
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
	 * most of the headers in {@link KafkaHeaders} are ever mapped as headers since they
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
	 * you generally should not map the {@code "id" and "timestamp"} headers. Note: most
	 * of the headers in {@link KafkaHeaders} are never mapped as headers since they
	 * represent data in consumer/producer records.
	 * @param objectMapper the object mapper.
	 * @param patterns the patterns.
	 * @see org.springframework.util.PatternMatchUtils#simpleMatch(String, String)
	 */
	public DefaultKafkaHeaderMapper(ObjectMapper objectMapper, String... patterns) {
		super(patterns);
		Assert.notNull(objectMapper, "'objectMapper' must not be null");
		Assert.noNullElements(patterns, "'patterns' must not have null elements");
		this.objectMapper = objectMapper;
		this.objectMapper
				.registerModule(new SimpleModule().addDeserializer(MimeType.class, new MimeTypeJsonDeserializer()));
	}

	/**
	 * Return the object mapper.
	 * @return the mapper.
	 */
	protected ObjectMapper getObjectMapper() {
		return this.objectMapper;
	}

	/**
	 * Provide direct access to the trusted packages set for subclasses.
	 * @return the trusted packages.
	 * @since 2.2
	 */
	protected Set<String> getTrustedPackages() {
		return this.trustedPackages;
	}

	/**
	 * Provide direct access to the toString() classes by subclasses.
	 * @return the toString() classes.
	 * @since 2.2
	 */
	protected Set<String> getToStringClasses() {
		return this.toStringClasses;
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

	/**
	 * Add class names that the outbound mapper should perform toString() operations on
	 * before mapping.
	 * @param classNames the class names.
	 * @since 2.2
	 */
	public void addToStringClasses(String... classNames) {
		this.toStringClasses.addAll(Arrays.asList(classNames));
	}

	@Override
	public void fromHeaders(MessageHeaders headers, Headers target) {
		final Map<String, String> jsonHeaders = new HashMap<>();
		final ObjectMapper headerObjectMapper = getObjectMapper();
		headers.forEach((k, v) -> {
			if (matches(k, v)) {
				if (v instanceof byte[]) {
					target.add(new RecordHeader(k, (byte[]) v));
				}
				else {
					try {
						Object value = v;
						String className = v.getClass().getName();
						if (this.toStringClasses.contains(className)) {
							value = v.toString();
							className = "java.lang.String";
						}
						target.add(new RecordHeader(k, headerObjectMapper.writeValueAsBytes(value)));
						jsonHeaders.put(k, className);
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
				target.add(new RecordHeader(JSON_TYPES, headerObjectMapper.writeValueAsBytes(jsonHeaders)));
			}
			catch (IllegalStateException | JsonProcessingException e) {
				logger.error("Could not add json types header", e);
			}
		}
	}

	@Override
	public void toHeaders(Headers source, final Map<String, Object> headers) {
		final Map<String, String> jsonTypes = decodeJsonTypes(source);
		final ObjectMapper headerObjectMapper = getObjectMapper();
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
							Object value = headerObjectMapper.readValue(h.value(), type);
							if (type.equals(NonTrustedHeaderType.class)) {
								// Upstream NTHT propagated; may be trusted here...
								NonTrustedHeaderType nth = (NonTrustedHeaderType) value;
								if (trusted(nth.getUntrustedType())) {
									try {
										type = ClassUtils.forName(nth.getUntrustedType(), null);
										value = headerObjectMapper.readValue(nth.getHeaderValue(), type);
									}
									catch (Exception e) {
										logger.error("Could not decode header: " + nth, e);
									}
								}
							}
							headers.put(h.key(), value);
						}
						catch (IOException e) {
							logger.error("Could not decode json type: " + new String(h.value()) + " for key: " + h
											.key(),
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

	@SuppressWarnings("unchecked")
	@Nullable
	private Map<String, String> decodeJsonTypes(Headers source) {
		Map<String, String> types = null;
		Iterator<Header> iterator = source.iterator();
		ObjectMapper headerObjectMapper = getObjectMapper();
		while (iterator.hasNext()) {
			Header next = iterator.next();
			if (next.key().equals(JSON_TYPES)) {
				try {
					types = headerObjectMapper.readValue(next.value(), Map.class);
				}
				catch (IOException e) {
					logger.error("Could not decode json types: " + new String(next.value()), e);
				}
				break;
			}
		}
		return types;
	}

	protected boolean trusted(String requestedType) {
		if (requestedType.equals(NonTrustedHeaderType.class.getName())) {
			return true;
		}
		if (!this.trustedPackages.isEmpty()) {
			int lastDot = requestedType.lastIndexOf('.');
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
	 * The {@link StdNodeBasedDeserializer} extension for {@link MimeType} deserialization.
	 * It is presented here for backward compatibility when older producers send {@link MimeType}
	 * headers as serialization version.
	 */
	private class MimeTypeJsonDeserializer extends StdNodeBasedDeserializer<MimeType> {

		private static final long serialVersionUID = 1L;

		MimeTypeJsonDeserializer() {
			super(MimeType.class);
		}

		@Override
		public MimeType convert(JsonNode root, DeserializationContext ctxt) throws IOException {
			JsonNode type = root.get("type");
			JsonNode subType = root.get("subtype");
			JsonNode parameters = root.get("parameters");
			Map<String, String> params =
					DefaultKafkaHeaderMapper.this.objectMapper.readValue(parameters.traverse(),
							TypeFactory.defaultInstance()
									.constructMapType(HashMap.class, String.class, String.class));
			return new MimeType(type.asText(), subType.asText(), params);
		}

	}

	/**
	 * Represents a header that could not be decoded due to an untrusted type.
	 */
	public static class NonTrustedHeaderType {

		private byte[] headerValue;

		private String untrustedType;

		public NonTrustedHeaderType() {
			super();
		}

		NonTrustedHeaderType(byte[] headerValue, String untrustedType) { // NOSONAR
			this.headerValue = headerValue; // NOSONAR
			this.untrustedType = untrustedType;
		}


		public void setHeaderValue(byte[] headerValue) {
			this.headerValue = headerValue; // NOSONAR array reference
		}

		public byte[] getHeaderValue() {
			return this.headerValue; // NOSONAR
		}

		public void setUntrustedType(String untrustedType) {
			this.untrustedType = untrustedType;
		}

		public String getUntrustedType() {
			return this.untrustedType;
		}

		@Override
		public String toString() {
			try {
				return "NonTrustedHeaderType [headerValue=" + new String(this.headerValue, StandardCharsets.UTF_8)
						+ ", untrustedType=" + this.untrustedType + "]";
			}
			catch (Exception e) {
				return "NonTrustedHeaderType [headerValue=" + Arrays.toString(this.headerValue) + ", untrustedType="
						+ this.untrustedType + "]";
			}
		}

	}

}
