package io.coherity.estoria.collector.engine.impl.util;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class JsonSupport
{
    private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

    private JsonSupport()
    {
    }

    private static ObjectMapper createObjectMapper()
    {
        ObjectMapper mapper = new ObjectMapper();

        mapper.findAndRegisterModules();

        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(com.fasterxml.jackson.databind.SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS, true);
        mapper.configure(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        return mapper;
    }

    public static ObjectMapper objectMapper()
    {
        return OBJECT_MAPPER;
    }

    
    public static String toJson(Object value)
    {
        try
        {
            return OBJECT_MAPPER.writeValueAsString(value);
        }
        catch (JsonProcessingException ex)
        {
            throw new JsonException("Failed to serialize object to JSON.", ex);
        }
    }
    
//    public static String toJson(Object value)
//    {
//    	if(value == null)
//    	{
//    		return "";
//    	}
//        try
//        {
//            return OBJECT_MAPPER.writeValueAsString(value);
//        }
//        catch (JsonProcessingException ex)
//        {
//            throw new JsonException("Failed to serialize object to JSON.", ex);
//        }
//    }

    public static String toJsonSilent(Object value)
    {
    	String result = "";
        try
        {
        	result = OBJECT_MAPPER.writeValueAsString(value);
        }
        catch (Throwable t)
        {
        	log.warn("could not serialize to json", t);
        }
        return result;
    }

    public static String toPrettyJson(Object value)
    {
        try
        {
            return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(value);
        }
        catch (JsonProcessingException ex)
        {
            throw new JsonException("Failed to serialize object to pretty JSON.", ex);
        }
    }

    public static <T> T fromJson(String json, Class<T> targetType)
    {
    	if(StringUtils.isEmpty(json))
    	{
    		return null;
    	}
        try
        {
            return OBJECT_MAPPER.readValue(json, targetType);
        }
        catch (JsonProcessingException ex)
        {
            throw new JsonException(
                "Failed to deserialize JSON into type: " + targetType.getName(),
                ex
            );
        }
    }

    public static <T> T fromJsonSilent(String json, Class<T> targetType)
    {
    	T result = null;
        try
        {
        	result = OBJECT_MAPPER.readValue(json, targetType);
        }
        catch (Throwable t)
        {
        	log.warn("could not process input json", t);
        }
        return result;
    }    

    public static ObjectReader readerFor(Class<?> targetType)
    {
        return OBJECT_MAPPER.readerFor(targetType);
    }

    public static ObjectWriter writerFor(Class<?> targetType)
    {
        return OBJECT_MAPPER.writerFor(targetType);
    }
    
    public static void writeJsonFile(String path, Object value) throws IOException
    {
        String json = JsonSupport.toJson(value);
        Files.writeString(Path.of(path), json, StandardCharsets.UTF_8);
    }
    
    
    public static JsonGenerator createJsonGenerator(Writer writer, boolean prettyFormat) throws IOException
    {
        JsonGenerator generator = OBJECT_MAPPER.getFactory().createGenerator(writer);

        if (prettyFormat)
        {
            DefaultPrettyPrinter prettyPrinter = new DefaultPrettyPrinter();
            DefaultIndenter indenter = new DefaultIndenter("  ", System.lineSeparator());
            prettyPrinter.indentObjectsWith(indenter);
            prettyPrinter.indentArraysWith(indenter);
            generator.setPrettyPrinter(prettyPrinter);
        }

        return generator;
    }
    
    public static void writeObject(JsonGenerator generator, Object value) throws IOException
    {
        OBJECT_MAPPER.writeValue(generator, value);
    }    
    
}