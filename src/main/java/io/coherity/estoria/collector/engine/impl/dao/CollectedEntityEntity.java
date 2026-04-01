package io.coherity.estoria.collector.engine.impl.dao;

import lombok.Data;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CollectedEntityEntity
{
	private String resultId;
	private long entityOrdinal;
	private String entityId;
	private String entityType;
	private String payloadJson;
}
