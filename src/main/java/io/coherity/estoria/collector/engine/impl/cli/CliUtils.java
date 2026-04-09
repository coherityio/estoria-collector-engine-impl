package io.coherity.estoria.collector.engine.impl.cli;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import io.coherity.estoria.collector.engine.api.CollectorEngine;
import io.coherity.estoria.collector.engine.impl.domain.CollectorEngineFactoryImpl;
import io.coherity.estoria.collector.engine.impl.util.JsonSupport;

final class CliUtils
{
    private CliUtils()
    {
    }

    static CollectorEngine createEngine()
    {
        return CollectorEngineFactoryImpl.getInstance().getCollectorEngine();
    }

    static <T> T readJsonIfPresent(String file, Class<T> type) throws IOException
    {
        if (file == null)
        {
            return null;
        }
        String json = readAllFromFile(file);
        return JsonSupport.fromJson(json, type);
    }

    static String readAllFromFile(String path) throws IOException
    {
        return Files.readString(Path.of(path), StandardCharsets.UTF_8);
    }

    static String readAllFromStdin() throws IOException
    {
        return new String(System.in.readAllBytes(), StandardCharsets.UTF_8);
    }

    static Set<String> parseCsvToSet(String csv)
    {
        if (csv == null || csv.isBlank())
        {
            return null;
        }
        return Arrays.stream(csv.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toSet());
    }
}
