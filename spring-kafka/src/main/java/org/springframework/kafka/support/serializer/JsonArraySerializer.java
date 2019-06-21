package org.springframework.kafka.support.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.List;
import java.util.Map;

public class JsonArraySerializer<T> implements Serializer<List<T>> {
    private final ObjectMapper objectMapper;

    public JsonArraySerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public JsonArraySerializer() {
        this(new ObjectMapper());
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, List<T> data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Can't serialize data [" + data + "] for topic [" + topic + "]", e);
        }
    }

    @Override
    public void close() {
    }
}