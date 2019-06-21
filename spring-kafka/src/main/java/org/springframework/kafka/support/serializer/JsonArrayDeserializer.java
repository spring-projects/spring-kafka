package org.springframework.kafka.support.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.core.ResolvableType;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class JsonArrayDeserializer<T> implements Deserializer<List<T>> {
    private final ObjectMapper objectMapper;
    private final Class<? super T> targetType;

    @SuppressWarnings("unchecked")
    public JsonArrayDeserializer(Class<? super T> targetType, ObjectMapper objectMapper) {
        this.targetType = targetType == null ? (Class<T>) ResolvableType.forClass(getClass()).getSuperType().resolveGeneric(0) : targetType;
        this.objectMapper = objectMapper;
    }

    public JsonArrayDeserializer() {
        this(null, new ObjectMapper());
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public List<T> deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, objectMapper.getTypeFactory().constructCollectionType(List.class, targetType));
        } catch (IOException e) {
            throw new SerializationException("Can't deserialize data [" + Arrays.toString(data) + "] from topic [" + topic + "]", e);
        }
    }

    @Override
    public void close() {
    }
}