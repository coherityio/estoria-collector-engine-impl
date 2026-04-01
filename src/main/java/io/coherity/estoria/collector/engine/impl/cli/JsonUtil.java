package io.coherity.estoria.collector.engine.impl.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

final class JsonUtil
{
    private static final ObjectMapper MAPPER = new ObjectMapper()
        .findAndRegisterModules()
        .enable(SerializationFeature.INDENT_OUTPUT);

    private JsonUtil()
    {
    }

    static String toJson(Object value) throws IOException
    {
        return MAPPER.writeValueAsString(value);
    }

    static <T> T fromJson(String json, Class<T> type) throws IOException
    {
        return MAPPER.readValue(json, type);
    }

    static void writeJsonFile(String path, Object value) throws IOException
    {
        String json = toJson(value);
        Files.writeString(Path.of(path), json, StandardCharsets.UTF_8);
    }
}
