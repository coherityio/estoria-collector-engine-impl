package io.coherity.estoria.collector.engine.impl.domain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.coherity.estoria.collector.provider.aws.TestCollector;
import io.coherity.estoria.collector.provider.aws.VpcCollector;
import io.coherity.estoria.collector.spi.Collector;

public class CollectorRegistryImpl implements CollectorRegistry
{
    private final Map<String, Collector> collectorMap;
    private final Map<String, Set<String>> dependencyGraph;
    private final Set<String> knownEntityTypes;

    public CollectorRegistryImpl()
    {
        this.collectorMap = new LinkedHashMap<>();
        this.dependencyGraph = new LinkedHashMap<>();
        this.knownEntityTypes = new LinkedHashSet<>();
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
            requires = Set.of();
        }

        Set<String> dependencyCopy = new LinkedHashSet<>();
        for (String dependency : requires)
        {
            if (dependency != null && !dependency.isBlank())
            {
                dependencyCopy.add(dependency);
            }
        }

        collectorMap.put(entityType, collector);
        dependencyGraph.put(entityType, dependencyCopy);
        knownEntityTypes.add(entityType);
        knownEntityTypes.addAll(dependencyCopy);
    }

    public synchronized void registerAll(Collection<Collector> collectors)
    {
        if (collectors == null)
        {
            return;
        }

        for (Collector collector : collectors)
        {
            register(collector);
        }
    }

    public synchronized Set<String> getRegisteredEntityTypes()
    {
        return Collections.unmodifiableSet(new LinkedHashSet<>(collectorMap.keySet()));
    }

    public synchronized Optional<Collector> getCollector(String entityType)
    {
		if (entityType == null || entityType.isBlank() || !collectorMap.containsKey(entityType))
		{
			return Optional.empty();
		}

        return Optional.of(collectorMap.get(entityType));
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
        return Collections.unmodifiableCollection(new ArrayList<>(collectorMap.values()));
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

        Set<String> dependencies = dependencyGraph.get(entityType);
        if (dependencies == null)
        {
            return null;
        }

        return Collections.unmodifiableSet(new LinkedHashSet<>(dependencies));
    }

    public synchronized Set<String> getKnownEntityTypesComplement(Set<String> entityTypes)
    {
        if (entityTypes == null || entityTypes.isEmpty())
        {
            return new LinkedHashSet<>(knownEntityTypes);
        }

        Set<String> difference = new LinkedHashSet<>(knownEntityTypes);
        difference.removeAll(entityTypes);
        return difference;
    }

    public synchronized Set<String> getKnownEntityTypes()
    {
        return new LinkedHashSet<>(knownEntityTypes);
    }

    public synchronized Set<String> getUnresolvedEntityTypes()
    {
        Set<String> unresolved = new LinkedHashSet<>(knownEntityTypes);
        unresolved.removeAll(collectorMap.keySet());
        return unresolved;
    }

    public List<String> getExecutionOrder(String entityType) throws CircularReferenceException
    {
        Map<String, Set<String>> graphSnapshot;
        synchronized (this)
        {
            graphSnapshot = new LinkedHashMap<>();
            for (Map.Entry<String, Set<String>> entry : dependencyGraph.entrySet())
            {
                graphSnapshot.put(entry.getKey(), new LinkedHashSet<>(entry.getValue()));
            }
        }

        Set<String> visited = new LinkedHashSet<>();
        Set<String> visiting = new HashSet<>();
        List<String> result = new ArrayList<>();

        if (entityType == null || entityType.isBlank())
        {
            for (String registeredEntityType : graphSnapshot.keySet())
            {
                if (!visited.contains(registeredEntityType))
                {
                    dfsTraversal(registeredEntityType, visited, visiting, result, graphSnapshot);
                }
            }
        }
        else
        {
            dfsTraversal(entityType, visited, visiting, result, graphSnapshot);
        }

        return result;
    }

    public static void dfsTraversal(
        String entityType,
        Set<String> visited,
        Set<String> visiting,
        List<String> result,
        Map<String, Set<String>> graph) throws CircularReferenceException
    {
        if (visited.contains(entityType))
        {
            return;
        }

        if (visiting.contains(entityType))
        {
            throw new CircularReferenceException(
                "Circular dependency detected involving entity type '" + entityType + "'");
        }

        visiting.add(entityType);

        Set<String> dependencies = graph.getOrDefault(entityType, Set.of());
        for (String dependency : dependencies)
        {
            if (graph.containsKey(dependency))
            {
                dfsTraversal(dependency, visited, visiting, result, graph);
            }
        }

        visiting.remove(entityType);
        visited.add(entityType);
        result.add(entityType);
    }
    
    
    public static void main(String[] args)
	{
		CollectorRegistryImpl registry = new CollectorRegistryImpl();
		Collector vpcCollector = new VpcCollector();
		Collector testCollector = new TestCollector();

		registry.register(vpcCollector);
		registry.register(testCollector);

		List<String> orderedEntityTypes = registry.getExecutionOrder(null);
		
		print(orderedEntityTypes);

    }
    
    public static void print(List<String> elements)
    {
    	if(elements != null)
    	{
    		elements.forEach(e -> {System.out.println(e);});
    	}
    }

}
