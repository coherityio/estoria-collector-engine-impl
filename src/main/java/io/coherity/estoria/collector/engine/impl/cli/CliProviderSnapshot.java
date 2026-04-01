package io.coherity.estoria.collector.engine.impl.cli;

import io.coherity.estoria.collector.engine.api.CloudEntityPage;
import io.coherity.estoria.collector.engine.api.ProviderSnapshot;
import io.coherity.estoria.collector.engine.api.ProviderSnapshotSummary;
import io.coherity.estoria.collector.spi.CollectionScope;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Minimal ProviderSnapshot implementation for CLI-driven snapshot builds.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class CliProviderSnapshot implements ProviderSnapshot
{
    private String providerId;
    private CollectionScope collectionScope;
    private ProviderSnapshotSummary providerSnapshotSummary;

    @Override
    public String getProviderId()
    {
        return providerId;
    }

    @Override
    public CollectionScope getCollectionScope()
    {
        return collectionScope;
    }

    @Override
    public ProviderSnapshotSummary getProviderSnapshotSummary()
    {
        return providerSnapshotSummary;
    }

    @Override
    public long getEntityCount()
    {
        return 0L;
    }

    @Override
    public long getEntityCountByType(String entityType)
    {
        return 0L;
    }

    @Override
    public CloudEntityPage getCloudEntityPage(String cursorToken, Integer pageSize)
    {
        throw new UnsupportedOperationException("CLI snapshot does not expose entity pages on ProviderSnapshot; use CollectionResult instead.");
    }

    @Override
    public CloudEntityPage getCloudEntityPageByType(String entityType, String cursorToken, Integer pageSize)
    {
        throw new UnsupportedOperationException("CLI snapshot does not expose entity pages on ProviderSnapshot; use CollectionResult instead.");
    }
}
