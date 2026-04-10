package io.coherity.estoria.collector.engine.impl.cli;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonGenerator;

import io.coherity.estoria.collector.engine.api.CloudEntityPage;
import io.coherity.estoria.collector.engine.api.CollectionFailure;
import io.coherity.estoria.collector.engine.api.CollectionResult;
import io.coherity.estoria.collector.engine.api.CollectionRun;
import io.coherity.estoria.collector.engine.api.CollectionRunFailure;
import io.coherity.estoria.collector.engine.api.CollectionRunSummary;
import io.coherity.estoria.collector.engine.api.CollectionSummary;
import io.coherity.estoria.collector.engine.api.CollectorEngine;
import io.coherity.estoria.collector.engine.impl.domain.CollectorEngineFactoryImpl;
import io.coherity.estoria.collector.engine.impl.util.JsonSupport;
import io.coherity.estoria.collector.spi.CloudEntity;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CliUtils
{
    private CliUtils()
    {
    }

    public static CollectorEngine createEngine()
    {
        return CollectorEngineFactoryImpl.getInstance().getCollectorEngine();
    }

    public static <T> T readJsonIfPresent(String file, Class<T> type) throws IOException
    {
        if (file == null)
        {
            return null;
        }
        try
        {
            String json = readAllFromFile(file);
            return JsonSupport.fromJson(json, type);
        }
        catch(FileNotFoundException fnfe)
        {
        	log.warn("could not find file: " + file, fnfe);
        }
        return null;
    }

    public static String readAllFromFile(String path) throws IOException
    {
        if (Files.exists(Path.of(path)))
        {
            return Files.readString(Path.of(path), StandardCharsets.UTF_8);
        }

        InputStream is = Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream(path);

        if (is != null)
        {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }

        throw new FileNotFoundException("Not found on filesystem or classpath: " + path);
    }

    public static String readAllFromStdin() throws IOException
    {
        return new String(System.in.readAllBytes(), StandardCharsets.UTF_8);
    }

    public static Set<String> parseCsvToSet(String csv)
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

    
    
    public static void streamCollectionRun(
            CollectionRun run,
            List<CollectionResult> results,
            int pageSize,
            String fileName, 
            boolean prettyFormat) throws IOException
    {
    	CliUtils.streamCollectionRun(run, results, pageSize, CliUtils.openWriter(fileName), prettyFormat);
    }
    
    
	public static void streamCollectionRun(
			CollectionRun run, 
			List<CollectionResult> results, 
			int pageSize,
			Writer writer, boolean prettyFormat) throws IOException
	{
		JsonGenerator generator = JsonSupport.createJsonGenerator(writer, prettyFormat);

		CollectionRunSummary runSummary = run.getCollectionRunSummary();
		CollectionRunFailure runFailure = run.getCollectionRunFailure();

		generator.writeStartObject();

		generator.writeStringField("runId", run.getRunId());
		generator.writeStringField("providerId", run.getProviderId());

		if (run.getProviderContext() != null)
		{
			generator.writeFieldName("providerContext");
			generator.writeObject(run.getProviderContext());
		}

		if (run.getStatus() != null)
		{
			generator.writeStringField("status", run.getStatus().name());
		} else
		{
			generator.writeNullField("status");
		}

		generator.writeFieldName("runStartTime");
		generator.writeObject(run.getRunStartTime());

		generator.writeFieldName("runEndTime");
		generator.writeObject(run.getRunEndTime());

		if (runSummary != null)
		{
			generator.writeFieldName("summary");
			generator.writeObject(runSummary);
		}

		if (runFailure != null)
		{
			generator.writeFieldName("failure");
			generator.writeObject(runFailure);
		}

		generator.writeArrayFieldStart("results");

		for (CollectionResult result : results)
		{
			CollectionFailure resultFailure = result.getCollectionFailure();
			CollectionSummary resultSummary = result.getCollectionSummary();

			generator.writeStartObject();

			generator.writeStringField("resultId", result.getResultId());
			generator.writeStringField("collectorId", result.getCollectorId());
			generator.writeStringField("entityType", result.getEntityType());
			generator.writeNumberField("entityCount", result.getEntityCount());
			generator.writeBooleanField("success", result.isSuccess());

			generator.writeFieldName("collectionStartTime");
			generator.writeObject(result.getCollectionStartTime());

			generator.writeFieldName("collectionEndTime");
			generator.writeObject(result.getCollectionEndTime());

			if (resultFailure != null)
			{
				generator.writeFieldName("failure");
				generator.writeObject(resultFailure);
			}

			if (resultSummary != null)
			{
				generator.writeFieldName("summary");
				generator.writeObject(resultSummary);
			}

			generator.writeArrayFieldStart("entities");

			String cursorToken = null;
			do
			{
				CloudEntityPage page = result.getCloudEntityPage(cursorToken, pageSize);

				if (page == null || page.getEntities() == null || page.getEntities().isEmpty())
				{
					break;
				}

				for (CloudEntity entity : page.getEntities())
				{
					generator.writeObject(entity);
				}

				generator.flush();
				cursorToken = page.getNextCursorToken();
			} while (cursorToken != null && !cursorToken.isBlank());

			generator.writeEndArray();
			generator.writeEndObject();
			generator.flush();

			log.debug("Streamed result entityType={} resultId={}", result.getEntityType(), result.getResultId());
		}

		generator.writeEndArray();
		generator.writeEndObject();
		generator.writeRaw(System.lineSeparator());
		generator.flush();
	}    
    
    
    
    
//    public static void streamCollectionRun(
//        CollectionRun run,
//        List<CollectionResult> results,
//        int pageSize,
//        Writer writer) throws IOException
//    {
//        CollectionRunSummary runSummary  = run.getCollectionRunSummary();
//        CollectionRunFailure runFailure  = run.getCollectionRunFailure();
//
//        // --- run header --------------------------------------------------
//        writer.write("{");
//        writer.write("\"runId\":"        + JsonSupport.toJson(run.getRunId())      + ",");
//        writer.write("\"providerId\":"   + JsonSupport.toJson(run.getProviderId()) + ",");
//        
//        if(run.getProviderContext() != null)
//        {
//            writer.write("\"providerContext\":"   + JsonSupport.toJson(run.getProviderContext()) + ",");
//        }
//        
//        writer.write("\"status\":"       + JsonSupport.toJson(run.getStatus() != null ? run.getStatus().name() : null) + ",");
//        writer.write("\"runStartTime\":" + JsonSupport.toJson(run.getRunStartTime()) + ",");
//        writer.write("\"runEndTime\":"   + JsonSupport.toJson(run.getRunEndTime())   + ",");
//
//        if (runSummary != null)
//        {
//            writer.write("\"summary\":"  + JsonSupport.toJson(runSummary) + ",");
//        }
//
//        if (runFailure != null)
//        {
//            writer.write("\"failure\":"  + JsonSupport.toJson(runFailure) + ",");
//        }
//
//        writer.write("\"results\":[");
//        writer.flush();
//
//        // --- one result at a time ----------------------------------------
//        int resultIndex = 0;
//        for (CollectionResult result : results)
//        {
//            if (resultIndex > 0)
//            {
//                writer.write(",");
//            }
//
//            CollectionFailure resultFailure  = result.getCollectionFailure();
//            CollectionSummary resultSummary  = result.getCollectionSummary();
//
//            // result header — entity array opened but not yet written
//            writer.write("{");
//            writer.write("\"resultId\":"    + JsonSupport.toJson(result.getResultId())    + ",");
//            writer.write("\"collectorId\":" + JsonSupport.toJson(result.getCollectorId()) + ",");
//            writer.write("\"entityType\":"  + JsonSupport.toJson(result.getEntityType())  + ",");
//            writer.write("\"entityCount\":" + result.getEntityCount()                     + ",");
//            writer.write("\"success\":"     + result.isSuccess()                          + ",");
//            writer.write("\"collectionStartTime\":"
//                + JsonSupport.toJson(result.getCollectionStartTime()) + ",");
//            writer.write("\"collectionEndTime\":"
//                + JsonSupport.toJson(result.getCollectionEndTime())   + ",");
//
//            if (resultFailure != null)
//            {
//                writer.write("\"failure\":"  + JsonSupport.toJson(resultFailure) + ",");
//            }
//
//            if (resultSummary != null)
//            {
//                writer.write("\"summary\":"  + JsonSupport.toJson(resultSummary) + ",");
//            }
//
//            writer.write("\"entities\":[");
//            writer.flush();
//
//            // --- entity pages for this result ----------------------------
//            // Cursor-based: start with null, advance using nextCursorToken.
//            String cursorToken = null;
//            boolean firstEntity = true;
//
//            do
//            {
//                CloudEntityPage page = result.getCloudEntityPage(cursorToken, pageSize);
//
//                if (page == null || page.getEntities() == null || page.getEntities().isEmpty())
//                {
//                    break;
//                }
//
//                for (CloudEntity entity : page.getEntities())
//                {
//                    if (!firstEntity)
//                    {
//                        writer.write(",");
//                    }
//                    // Serialize and immediately discard — no accumulation.
//                    writer.write(JsonSupport.toJson(entity));
//                    firstEntity = false;
//                }
//
//                // Flush every page to the target immediately.
//                writer.flush();
//
//                cursorToken = page.getNextCursorToken();
//            }
//            while (cursorToken != null && !cursorToken.isBlank());
//
//            // close entities array and result object
//            writer.write("]}");
//            writer.flush();
//
//            log.debug("Streamed result entityType={} resultId={}",
//                result.getEntityType(), result.getResultId());
//
//            resultIndex++;
//        }
//
//        // close results array and root object
//        writer.write("]}");
//        writer.write(System.lineSeparator());
//        writer.flush();
//    }

    public static Writer openWriter(String outputFile) throws IOException
    {
        if (outputFile != null && !outputFile.isBlank())
        {
            log.info("Writing collection output to file: {}", outputFile);
            return Files.newBufferedWriter(Path.of(outputFile), StandardCharsets.UTF_8);
        }

        return new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8))
        {
            @Override
            public void close() throws IOException
            {
                flush(); // flush but never close stdout
            }
        };
    }

}
