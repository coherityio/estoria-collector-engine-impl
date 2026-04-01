package io.coherity.estoria.collector.engine.impl.domain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.coherity.estoria.collector.spi.Collector;

public class CollectorRegistry
{
    private final Map<String, Collector> collectorMap;
    private final Map<String, Set<String>> dependencyGraph;
    private final Set<String> knownEntityTypes;

    public CollectorRegistry()
    {
        this.collectorMap = new HashMap<>();
        this.dependencyGraph = new HashMap<>();
        this.knownEntityTypes = new HashSet<>();
    }

    public synchronized void register(Collector collector)
    {
        if (collector == null)
        {
            return;
        }

        String entityType = collector.getEntityType();
        if (entityType == null || entityType.isBlank())
        {
            return;
        }

        Set<String> requires = collector.requiresEntityTypes();
        if (requires == null)
        {
            requires = new HashSet<>();
        }

        collectorMap.put(entityType, collector);
        dependencyGraph.put(entityType, new HashSet<>(requires));
        knownEntityTypes.add(entityType);
        knownEntityTypes.addAll(requires);
    }

    public synchronized void registerAll(Collection<Collector> collectors)
    {
        collectors.forEach(this::register);
    }

    public synchronized Set<String> getRegisteredEntityTypes()
    {
    	return this.collectorMap.keySet();
    }
    
    public synchronized Collector getCollector(String entityType)
    {
        if (entityType == null || entityType.isBlank())
        {
            return null;
        }
        return collectorMap.get(entityType);
    }

    public synchronized boolean hasCollector(String entityType)
    {
        if (entityType == null || entityType.isBlank())
        {
            return false;
        }
        return collectorMap.containsKey(entityType);
    }

    public synchronized Collection<Collector> getCollectors()
    {
        return Collections.unmodifiableCollection(collectorMap.values());
    }

    public synchronized int size()
    {
        return collectorMap.size();
    }
    
    public synchronized Set<String> getDependencies(String entityType)
    {
        if (entityType == null || entityType.isBlank())
        {
        	return Set.of();
        }
        Set<String> deps = this.dependencyGraph.get(entityType);
        if (deps == null)
        {
        	return null;
        }
        return Collections.unmodifiableSet(deps);
    }
    
    public synchronized Set<String> getKnownEntityTypesComplement(Set<String> entityTypes)
    {
    	if (entityTypes == null || entityTypes.isEmpty())
    	{
    		return new HashSet<>(knownEntityTypes);
    	}
    	Set<String> diff = new HashSet<>(knownEntityTypes);
    	diff.removeAll(entityTypes);
    	return diff;
    }

    public synchronized Set<String> getKnownEntityTypes()
    {
        return new HashSet<>(knownEntityTypes);
    }

    public synchronized Set<String> getUnresolvedEntityTypes()
    {
        Set<String> unresolved = new HashSet<>(knownEntityTypes);
        unresolved.removeAll(collectorMap.keySet());
        return unresolved;
    }
    
    // ===============================================================
    // UTILITY METHODS
    // ===============================================================
    public List<String> getExecutionOrder(String entityType) throws CircularReferenceException
    {
        Map<String, Set<String>> graphSnapshot;
        synchronized (this)
        {
            graphSnapshot = new HashMap<>();
            for (Map.Entry<String, Set<String>> entry : dependencyGraph.entrySet())
            {
                graphSnapshot.put(entry.getKey(), new HashSet<>(entry.getValue()));
            }
        }

        Set<String> visited = new LinkedHashSet<>();
        Set<String> visiting = new HashSet<>();
        List<String> result = new ArrayList<>();

        CollectorRegistry.dfsTraversalHelper(entityType, visited, visiting, result, graphSnapshot);
        return result;
    }

    public static void dfsTraversalHelper(String entityType, Set<String> visited, Set<String> visiting, List<String> result, Map<String, Set<String>> graph) throws CircularReferenceException
    {
        if (visited.contains(entityType))
        {
            return;
        }

        if (visiting.contains(entityType))
        {
            throw new CircularReferenceException("Circular dependency detected involving entity type '" + entityType + "'");
        }

        visiting.add(entityType);

        Set<String> dependencies = graph.getOrDefault(entityType, new HashSet<>());
        for (String dependency : dependencies)
        {
            if (graph.containsKey(dependency))
            {
                dfsTraversalHelper(dependency, visited, visiting, result, graph);
            }
        }

        visiting.remove(entityType);
        visited.add(entityType);
        result.add(entityType);
    }

}
