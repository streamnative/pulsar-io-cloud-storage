package org.apache.pulsar.ecosystem.io.s3.format;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteSource;
import org.apache.pulsar.functions.api.Record;

import java.nio.charset.StandardCharsets;

public class JsonFormat<C, T> implements Format<C, Record<T>> {

    @Override
    public String getExtension() {
        return ".json";
    }

    @Override
    public ByteSource recordWriter(C config, Record<T> record) {
//        record.getValue()
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return ByteSource.wrap(objectMapper.writeValueAsString(record.getValue()).getBytes(StandardCharsets.UTF_8));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
