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
public class CollectionRunEntity
{
	private String runId;
	private String providerId;
	private String providerContext;
	private String status;
	private Instant runStartTime;
	private Instant runEndTime;
	private long totalCollectorCount;
	private long successfulCollectorCount;
	private long failedCollectorCount;
	private long totalEntityCount;
	private String failureMessage;
	private String failureExceptionClass;
}
