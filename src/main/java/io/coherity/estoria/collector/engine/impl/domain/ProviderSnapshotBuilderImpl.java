package io.coherity.estoria.collector.engine.impl.domain;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import io.coherity.estoria.collector.engine.api.CloudEntityPage;
import io.coherity.estoria.collector.engine.api.CollectionResult;
import io.coherity.estoria.collector.engine.api.ProviderSnapshot;
import io.coherity.estoria.collector.engine.api.SnapshotBuildException;
import io.coherity.estoria.collector.engine.api.SnapshotBuilder;
import io.coherity.estoria.collector.spi.CloudEntity;
import lombok.extern.slf4j.Slf4j;

/**
 * Thread-safe implementation of ProviderSnapshotBuilder that streams entities
 * from a {@link CollectionResult} to a JSON-lines file on disk.
 *
 * Each line is a wrapper object grouped by entity type, for example:
 * {"providerId":"aws","entityType":"EC2","entity":{...}}
 */
@Slf4j
public class ProviderSnapshotBuilderImpl implements SnapshotBuilder
{
	private final Path baseDir;

	public ProviderSnapshotBuilderImpl()
	{
		this(Paths.get("snapshots"));
	}

	public ProviderSnapshotBuilderImpl(Path baseDir)
	{
		this.baseDir = baseDir;
	}

	@Override
	public ProviderSnapshot mergeSnapshot(ProviderSnapshot providerSnapshot, CollectionResult collectionResult)
		throws SnapshotBuildException
	{
		if (providerSnapshot == null)
		{
			throw new SnapshotBuildException("providerSnapshot must not be null");
		}
		if (collectionResult == null)
		{
			throw new SnapshotBuildException("collectionResult must not be null");
		}

		try
		{
			Files.createDirectories(baseDir);
			Path file = resolveSnapshotFile(providerSnapshot, collectionResult);
			log.info("Writing provider snapshot for providerId={} entityType={} to file {}",
				providerSnapshot.getProviderId(), collectionResult.getEntityType(), file.toAbsolutePath());

			final int pageSize = 500;
			String cursor = null;

			try (BufferedWriter writer = Files.newBufferedWriter(
				file,
				StandardCharsets.UTF_8,
				StandardOpenOption.CREATE,
				StandardOpenOption.APPEND))
			{
				while (true)
				{
					CloudEntityPage page = collectionResult.getCloudEntityPage(cursor, pageSize);
					if (page == null || page.getEntities() == null || page.getEntities().isEmpty())
					{
						log.debug("No more entities to write for providerId={} entityType={}",
							providerSnapshot.getProviderId(), collectionResult.getEntityType());
						break;
					}

					for (CloudEntity entity : page.getEntities())
					{
						if (log.isTraceEnabled())
						{
							log.trace("Writing entity for providerId={} entityType={}",
								providerSnapshot.getProviderId(), collectionResult.getEntityType());
						}
						String jsonLine = buildJsonLine(providerSnapshot, collectionResult, entity);
						writer.write(jsonLine);
						writer.newLine();
					}

					cursor = page.getNextCursorToken();
					if (cursor == null)
					{
						log.debug("Reached end of cursor stream for providerId={} entityType={}",
							providerSnapshot.getProviderId(), collectionResult.getEntityType());
						break;
					}
				}
			}

			return providerSnapshot;
		}
		catch (IOException e)
		{
			throw new SnapshotBuildException("Failed to write provider snapshot file", e);
		}
	}

	private Path resolveSnapshotFile(ProviderSnapshot providerSnapshot, CollectionResult entitiesSnapshot)
	{
		String providerId = safe(providerSnapshot.getProviderId());
		String entityType = safe(entitiesSnapshot.getEntityType());
		String fileName = providerId + "-" + entityType + ".jsonl";
		return baseDir.resolve(fileName);
	}

	private String buildJsonLine(ProviderSnapshot providerSnapshot,
		CollectionResult result,
		CloudEntity entity)
	{
		String providerId = safe(providerSnapshot.getProviderId());
		String entityType = safe(result.getEntityType());
		String entityPayload = entity == null ? "" : safe(entity.toString());

		StringBuilder sb = new StringBuilder(256);
		sb.append('{');
		sb.append("\"providerId\":\"").append(escapeJson(providerId)).append('\"');
		sb.append(",\"entityType\":\"").append(escapeJson(entityType)).append('\"');
		sb.append(",\"entity\":");
		if (looksLikeJsonObject(entityPayload))
		{
			sb.append(entityPayload);
		}
		else
		{
			sb.append('"').append(escapeJson(entityPayload)).append('"');
		}
		sb.append('}');
		return sb.toString();
	}

	private static String safe(String s)
	{
		return s == null ? "" : s;
	}

	private static boolean looksLikeJsonObject(String s)
	{
		if (s == null)
		{
			return false;
		}
		String trimmed = s.trim();
		return trimmed.startsWith("{") && trimmed.endsWith("}");
	}

	private static String escapeJson(String s)
	{
		StringBuilder sb = new StringBuilder(s.length() + 16);
		for (int i = 0; i < s.length(); i++)
		{
			char c = s.charAt(i);
			switch (c)
			{
				case '"':
					sb.append("\\\"");
					break;
				case '\\':
					sb.append("\\\\");
					break;
				case '\b':
					sb.append("\\b");
					break;
				case '\f':
					sb.append("\\f");
					break;
				case '\n':
					sb.append("\\n");
					break;
				case '\r':
					sb.append("\\r");
					break;
				case '\t':
					sb.append("\\t");
					break;
				default:
					if (c < 0x20)
					{
						sb.append(String.format("\\u%04x", (int) c));
					}
					else
					{
						sb.append(c);
					}
			}
		}
		return sb.toString();
	}
}
