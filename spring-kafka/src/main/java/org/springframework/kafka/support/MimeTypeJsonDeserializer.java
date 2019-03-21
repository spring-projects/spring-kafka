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
import java.util.HashMap;
import java.util.Map;

import org.springframework.util.MimeType;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdNodeBasedDeserializer;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * {@link MimeType} deserializer.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 1.3
 *
 */
public class MimeTypeJsonDeserializer extends StdNodeBasedDeserializer<MimeType> {

	private static final long serialVersionUID = 1L;

	private final ObjectMapper mapper;

	public MimeTypeJsonDeserializer(ObjectMapper mapper) {
		super(MimeType.class);
		this.mapper = mapper;
	}

	@Override
	public MimeType convert(JsonNode root, DeserializationContext ctxt) throws IOException {
		JsonNode type = root.get("type");
		JsonNode subType = root.get("subtype");
		JsonNode parameters = root.get("parameters");
		Map<String, String> params = this.mapper.readValue(parameters.traverse(),
				TypeFactory.defaultInstance().constructMapType(HashMap.class, String.class, String.class));
		return new MimeType(type.asText(), subType.asText(), params);
	}

}
