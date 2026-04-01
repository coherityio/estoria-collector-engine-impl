package io.coherity.estoria.collector.engine.impl.dao;

import java.time.Instant;

import lombok.Data;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CollectionResultEntity
{
	private String resultId;
	private String runId;
	private String collectorId;
	private String entityType;
	private String status;
	private long entityCount;
	private Instant collectionStartTime;
	private Instant collectionEndTime;
	private String failureMessage;
	private String failureExceptionClass;
}
