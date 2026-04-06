package io.coherity.estoria.collector.engine.impl.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.coherity.estoria.collector.engine.api.CollectionPlan;
import io.coherity.estoria.collector.engine.api.PlanningException;
import io.coherity.estoria.collector.spi.CollectorContext;

class CollectionPlannerImplTest
{
	private static final String PROVIDER_ID = "aws";

	private CollectionPlannerImpl newPlanner(ProviderRegistry providerRegistry)
	{
		return new CollectionPlannerImpl(providerRegistry);
	}

	private CollectorContext collectorContext()
	{
		return CollectorContext.builder().build();
	}

	@Nested
	class PlanWithTargetsAndSkipsTest
	{
		@Test
		void givenNullProviderId_whenPlanCalled_thenThrowsNullPointerException()
		{
			CollectionPlannerImpl planner = newPlanner(mock(ProviderRegistry.class));

			assertThatThrownBy(() -> planner.plan(null, Set.of("vpc"), Set.of("subnet"), collectorContext()))
					.isInstanceOf(NullPointerException.class).hasMessageContaining("required: providerId");
		}

		@Test
		void givenEmptyProviderId_whenPlanCalled_thenThrowsIllegalArgumentException()
		{
			CollectionPlannerImpl planner = newPlanner(mock(ProviderRegistry.class));

			assertThatThrownBy(() -> planner.plan("", Set.of("vpc"), Set.of("subnet"), collectorContext()))
					.isInstanceOf(IllegalArgumentException.class).hasMessageContaining("required: providerId");
		}

		@Test
		void givenNullTargetEntityTypes_whenPlanCalled_thenThrowsNullPointerException()
		{
			CollectionPlannerImpl planner = newPlanner(mock(ProviderRegistry.class));

			assertThatThrownBy(() -> planner.plan(PROVIDER_ID, null, Set.of("subnet"), collectorContext()))
					.isInstanceOf(NullPointerException.class).hasMessageContaining("required: targetEntityTypes");
		}

		@Test
		void givenEmptyTargetEntityTypes_whenPlanCalled_thenThrowsIllegalArgumentException()
		{
			CollectionPlannerImpl planner = newPlanner(mock(ProviderRegistry.class));

			assertThatThrownBy(() -> planner.plan(PROVIDER_ID, Set.of(), Set.of("subnet"), collectorContext()))
					.isInstanceOf(IllegalArgumentException.class).hasMessageContaining("required: targetEntityTypes");
		}

		@Test
		void givenProviderRegistryDoesNotContainCollectorRegistry_whenPlanCalled_thenThrowsIllegalStateException()
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			when(providerRegistry.getLoadedCollectorRegistry(PROVIDER_ID)).thenReturn(Optional.empty());

			CollectionPlannerImpl planner = newPlanner(providerRegistry);

			assertThatThrownBy(() -> planner.plan(PROVIDER_ID, Set.of("vpc"), Set.of("subnet"), collectorContext()))
					.isInstanceOf(IllegalStateException.class)
					.hasMessageContaining("could not find collector registry");
		}

		@Test
		void givenSingleTargetAndNullSkipTypes_whenPlanCalled_thenPlanIncludesDependencyExecutionOrder()
				throws PlanningException, CircularReferenceException
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CollectorRegistryImpl collectorRegistry = mock(CollectorRegistryImpl.class);

			when(providerRegistry.getLoadedCollectorRegistry(PROVIDER_ID)).thenReturn(Optional.of(collectorRegistry));
			when(collectorRegistry.getExecutionOrder("vpc")).thenReturn(List.of("subnet", "vpc"));

			CollectionPlannerImpl planner = newPlanner(providerRegistry);
			CollectorContext collectorContext = collectorContext();

			CollectionPlan collectionPlan = planner.plan(PROVIDER_ID, Set.of("vpc"), null, collectorContext);

			assertThat(collectionPlan.getProviderId()).isEqualTo(PROVIDER_ID);
			assertThat(collectionPlan.getCollectorContext()).isSameAs(collectorContext);
			assertThat(collectionPlan.getTargetEntityTypes()).containsExactly("vpc");
			assertThat(collectionPlan.getSkippedEntityTypes()).isNull();
			assertThat(collectionPlan.getEntityTypeExecutionOrder()).containsExactly("subnet", "vpc");

			verify(collectorRegistry, times(1)).getExecutionOrder("vpc");
		}

		@Test
		void givenMultipleTargetsWithOverlappingDependencies_whenPlanCalled_thenExecutionOrderIsDeDuplicated()
				throws PlanningException, CircularReferenceException
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CollectorRegistryImpl collectorRegistry = mock(CollectorRegistryImpl.class);

			when(providerRegistry.getLoadedCollectorRegistry(PROVIDER_ID)).thenReturn(Optional.of(collectorRegistry));
			when(collectorRegistry.getExecutionOrder("instance")).thenReturn(List.of("vpc", "subnet", "instance"));
			when(collectorRegistry.getExecutionOrder("security-group")).thenReturn(List.of("vpc", "security-group"));

			CollectionPlannerImpl planner = newPlanner(providerRegistry);

			CollectionPlan collectionPlan = planner.plan(PROVIDER_ID, Set.of("instance", "security-group"), null,
					collectorContext());

			assertThat(collectionPlan.getTargetEntityTypes()).containsExactlyInAnyOrder("instance", "security-group");
			assertThat(collectionPlan.getSkippedEntityTypes()).isNull();
			assertThat(collectionPlan.getEntityTypeExecutionOrder()).containsExactlyInAnyOrder("vpc", "subnet",
					"instance", "security-group");
			assertThat(collectionPlan.getEntityTypeExecutionOrder()).doesNotHaveDuplicates();
		}

		@Test
		void givenSkipEntityTypesProvided_whenPlanCalled_thenSkippedTypesAreRemovedFromExecutionOrder()
				throws PlanningException, CircularReferenceException
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CollectorRegistryImpl collectorRegistry = mock(CollectorRegistryImpl.class);

			when(providerRegistry.getLoadedCollectorRegistry(PROVIDER_ID)).thenReturn(Optional.of(collectorRegistry));
			when(collectorRegistry.getExecutionOrder("instance")).thenReturn(List.of("vpc", "subnet", "instance"));

			CollectionPlannerImpl planner = newPlanner(providerRegistry);

			CollectionPlan collectionPlan = planner.plan(PROVIDER_ID, Set.of("instance"), Set.of("subnet"),
					collectorContext());

			assertThat(collectionPlan.getTargetEntityTypes()).containsExactly("instance");
			assertThat(collectionPlan.getSkippedEntityTypes()).containsExactly("subnet");
			assertThat(collectionPlan.getEntityTypeExecutionOrder()).containsExactly("vpc", "instance");
		}

		@Test
		void givenEmptySkipEntityTypes_whenPlanCalled_thenSkippedEntityTypesRemainNull()
				throws PlanningException, CircularReferenceException
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CollectorRegistryImpl collectorRegistry = mock(CollectorRegistryImpl.class);

			when(providerRegistry.getLoadedCollectorRegistry(PROVIDER_ID)).thenReturn(Optional.of(collectorRegistry));
			when(collectorRegistry.getExecutionOrder("instance")).thenReturn(List.of("vpc", "subnet", "instance"));

			CollectionPlannerImpl planner = newPlanner(providerRegistry);

			CollectionPlan collectionPlan = planner.plan(PROVIDER_ID, Set.of("instance"), Set.of(), collectorContext());

			assertThat(collectionPlan.getSkippedEntityTypes()).isNull();
			assertThat(collectionPlan.getEntityTypeExecutionOrder()).containsExactly("vpc", "subnet", "instance");
		}

		@Test
		void givenCircularReference_whenPlanCalled_thenPlanningExceptionIsThrown() throws CircularReferenceException
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CollectorRegistryImpl collectorRegistry = mock(CollectorRegistryImpl.class);

			when(providerRegistry.getLoadedCollectorRegistry(PROVIDER_ID)).thenReturn(Optional.of(collectorRegistry));
			when(collectorRegistry.getExecutionOrder("instance"))
					.thenThrow(new CircularReferenceException("cycle detected"));

			CollectionPlannerImpl planner = newPlanner(providerRegistry);

			assertThatThrownBy(() -> planner.plan(PROVIDER_ID, Set.of("instance"), null, collectorContext()))
					.isInstanceOf(PlanningException.class).hasMessageContaining("Circular reference detected")
					.hasCauseInstanceOf(CircularReferenceException.class);
		}
	}

	@Nested
	class PlanWithSkipAllDependenciesTest
	{
		@Test
		void givenNullProviderId_whenPlanCalled_thenThrowsNullPointerException()
		{
			CollectionPlannerImpl planner = newPlanner(mock(ProviderRegistry.class));

			assertThatThrownBy(() -> planner.plan(null, Set.of("instance"), true, collectorContext()))
					.isInstanceOf(NullPointerException.class).hasMessageContaining("required: providerId");
		}

		@Test
		void givenEmptyProviderId_whenPlanCalled_thenThrowsIllegalArgumentException()
		{
			CollectionPlannerImpl planner = newPlanner(mock(ProviderRegistry.class));

			assertThatThrownBy(() -> planner.plan("", Set.of("instance"), true, collectorContext()))
					.isInstanceOf(IllegalArgumentException.class).hasMessageContaining("required: providerId");
		}

		@Test
		void givenNullTargetEntityTypes_whenPlanCalled_thenThrowsNullPointerException()
		{
			CollectionPlannerImpl planner = newPlanner(mock(ProviderRegistry.class));

			assertThatThrownBy(() -> planner.plan(PROVIDER_ID, null, true, collectorContext()))
					.isInstanceOf(NullPointerException.class).hasMessageContaining("required: targetEntityTypes");
		}

		@Test
		void givenEmptyTargetEntityTypes_whenPlanCalled_thenThrowsIllegalArgumentException()
		{
			CollectionPlannerImpl planner = newPlanner(mock(ProviderRegistry.class));

			assertThatThrownBy(() -> planner.plan(PROVIDER_ID, Set.of(), true, collectorContext()))
					.isInstanceOf(IllegalArgumentException.class).hasMessageContaining("required: targetEntityTypes");
		}

		@Test
		void givenProviderRegistryDoesNotContainCollectorRegistry_whenPlanCalled_thenThrowsIllegalStateException()
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			when(providerRegistry.getLoadedCollectorRegistry(PROVIDER_ID)).thenReturn(Optional.empty());

			CollectionPlannerImpl planner = newPlanner(providerRegistry);

			assertThatThrownBy(() -> planner.plan(PROVIDER_ID, Set.of("instance"), true, collectorContext()))
					.isInstanceOf(IllegalStateException.class)
					.hasMessageContaining("could not find collector registry");
		}

		@Test
		void givenSkipAllDependenciesTrue_whenPlanCalled_thenComplementIsSkipped()
				throws PlanningException, CircularReferenceException
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CollectorRegistryImpl collectorRegistry = mock(CollectorRegistryImpl.class);

			when(providerRegistry.getLoadedCollectorRegistry(PROVIDER_ID)).thenReturn(Optional.of(collectorRegistry));
			when(collectorRegistry.getKnownEntityTypesComplement(Set.of("instance")))
					.thenReturn(Set.of("vpc", "subnet"));
			when(collectorRegistry.getExecutionOrder("instance")).thenReturn(List.of("vpc", "subnet", "instance"));

			CollectionPlannerImpl planner = newPlanner(providerRegistry);

			CollectionPlan collectionPlan = planner.plan(PROVIDER_ID, Set.of("instance"), true, collectorContext());

			assertThat(collectionPlan.getTargetEntityTypes()).containsExactly("instance");
			assertThat(collectionPlan.getSkippedEntityTypes()).containsExactlyInAnyOrder("vpc", "subnet");
			assertThat(collectionPlan.getEntityTypeExecutionOrder()).containsExactly("instance");

			verify(collectorRegistry, times(1)).getKnownEntityTypesComplement(Set.of("instance"));
			verify(collectorRegistry, times(1)).getExecutionOrder("instance");
		}

		@Test
		void givenSkipAllDependenciesFalse_whenPlanCalled_thenComplementIsNotComputed()
				throws PlanningException, CircularReferenceException
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CollectorRegistryImpl collectorRegistry = mock(CollectorRegistryImpl.class);

			when(providerRegistry.getLoadedCollectorRegistry(PROVIDER_ID)).thenReturn(Optional.of(collectorRegistry));
			when(collectorRegistry.getExecutionOrder("instance")).thenReturn(List.of("vpc", "subnet", "instance"));

			CollectionPlannerImpl planner = newPlanner(providerRegistry);

			CollectionPlan collectionPlan = planner.plan(PROVIDER_ID, Set.of("instance"), false, collectorContext());

			assertThat(collectionPlan.getSkippedEntityTypes()).isNull();
			assertThat(collectionPlan.getEntityTypeExecutionOrder()).containsExactly("vpc", "subnet", "instance");

			verify(collectorRegistry, never()).getKnownEntityTypesComplement(Set.of("instance"));
			verify(collectorRegistry, times(1)).getExecutionOrder("instance");
		}
	}

	@Nested
	class PlanWithSkipSetOnlyTest
	{
		@Test
		void givenProviderRegistryDoesNotContainCollectorRegistry_whenPlanCalled_thenThrowsIllegalStateException()
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			when(providerRegistry.getLoadedCollectorRegistry(PROVIDER_ID)).thenReturn(Optional.empty());

			CollectionPlannerImpl planner = newPlanner(providerRegistry);

			assertThatThrownBy(() -> planner.plan(PROVIDER_ID, Set.of("subnet"), collectorContext()))
					.isInstanceOf(IllegalStateException.class)
					.hasMessageContaining("could not find collector registry");
		}

		@Test
		void givenNullSkipTypes_whenPlanCalled_thenAllRegisteredTypesBecomeTargets()
				throws PlanningException, CircularReferenceException
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CollectorRegistryImpl collectorRegistry = mock(CollectorRegistryImpl.class);

			when(providerRegistry.getLoadedCollectorRegistry(PROVIDER_ID)).thenReturn(Optional.of(collectorRegistry));
			when(collectorRegistry.getRegisteredEntityTypes()).thenReturn(Set.of("vpc", "subnet"));
			when(collectorRegistry.getExecutionOrder("vpc")).thenReturn(List.of("vpc"));
			when(collectorRegistry.getExecutionOrder("subnet")).thenReturn(List.of("vpc", "subnet"));

			CollectionPlannerImpl planner = newPlanner(providerRegistry);

			CollectionPlan collectionPlan = planner.plan(PROVIDER_ID, (Set<String>) null, collectorContext());

			assertThat(collectionPlan.getTargetEntityTypes()).containsExactlyInAnyOrder("vpc", "subnet");
			assertThat(collectionPlan.getSkippedEntityTypes()).isNull();
			assertThat(collectionPlan.getEntityTypeExecutionOrder()).containsExactly("vpc", "subnet");
		}

		@Test
		void givenSkipTypes_whenPlanCalled_thenTargetsAreDerivedFromRegisteredTypes()
				throws PlanningException, CircularReferenceException
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CollectorRegistryImpl collectorRegistry = mock(CollectorRegistryImpl.class);

			when(providerRegistry.getLoadedCollectorRegistry(PROVIDER_ID)).thenReturn(Optional.of(collectorRegistry));
			when(collectorRegistry.getRegisteredEntityTypes()).thenReturn(Set.of("vpc", "subnet", "instance"));
			when(collectorRegistry.getExecutionOrder("vpc")).thenReturn(List.of("vpc"));
			when(collectorRegistry.getExecutionOrder("instance")).thenReturn(List.of("vpc", "subnet", "instance"));

			CollectionPlannerImpl planner = newPlanner(providerRegistry);

			CollectionPlan collectionPlan = planner.plan(PROVIDER_ID, Set.of("subnet"), collectorContext());

			assertThat(collectionPlan.getTargetEntityTypes()).containsExactlyInAnyOrder("vpc", "instance");
			assertThat(collectionPlan.getSkippedEntityTypes()).containsExactly("subnet");
			assertThat(collectionPlan.getEntityTypeExecutionOrder()).containsExactly("vpc", "instance");
		}

		@Test
		void givenAllRegisteredEntityTypesSkipped_whenPlanCalled_thenThrowsIllegalArgumentException()
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CollectorRegistryImpl collectorRegistry = mock(CollectorRegistryImpl.class);

			when(providerRegistry.getLoadedCollectorRegistry(PROVIDER_ID)).thenReturn(Optional.of(collectorRegistry));
			when(collectorRegistry.getRegisteredEntityTypes()).thenReturn(Set.of("vpc", "subnet"));

			CollectionPlannerImpl planner = newPlanner(providerRegistry);

			assertThatThrownBy(() -> planner.plan(PROVIDER_ID, Set.of("vpc", "subnet"), collectorContext()))
					.isInstanceOf(IllegalArgumentException.class).hasMessageContaining("no target entity types remain");
		}

		@Test
		void givenEmptySkipSet_whenPlanCalled_thenAllRegisteredEntityTypesBecomeTargets()
				throws PlanningException, CircularReferenceException
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CollectorRegistryImpl collectorRegistry = mock(CollectorRegistryImpl.class);

			when(providerRegistry.getLoadedCollectorRegistry(PROVIDER_ID)).thenReturn(Optional.of(collectorRegistry));
			when(collectorRegistry.getRegisteredEntityTypes()).thenReturn(Set.of("vpc"));
			when(collectorRegistry.getExecutionOrder("vpc")).thenReturn(List.of("vpc"));

			CollectionPlannerImpl planner = newPlanner(providerRegistry);

			CollectionPlan collectionPlan = planner.plan(PROVIDER_ID, Set.of(), collectorContext());

			assertThat(collectionPlan.getTargetEntityTypes()).containsExactly("vpc");
			assertThat(collectionPlan.getSkippedEntityTypes()).isNull();
			assertThat(collectionPlan.getEntityTypeExecutionOrder()).containsExactly("vpc");
		}
	}

	@Nested
	class PlanAllTypesTest
	{
		@Test
		void givenProviderRegistryDoesNotContainCollectorRegistry_whenPlanCalled_thenThrowsIllegalStateException()
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			when(providerRegistry.getLoadedCollectorRegistry(PROVIDER_ID)).thenReturn(Optional.empty());

			CollectionPlannerImpl planner = newPlanner(providerRegistry);

			assertThatThrownBy(() -> planner.plan(PROVIDER_ID, collectorContext()))
					.isInstanceOf(IllegalStateException.class)
					.hasMessageContaining("could not find collector registry");
		}

		@Test
		void givenNoCollectorsRegistered_whenPlanCalled_thenThrowsIllegalStateException()
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CollectorRegistryImpl collectorRegistry = mock(CollectorRegistryImpl.class);

			when(providerRegistry.getLoadedCollectorRegistry(PROVIDER_ID)).thenReturn(Optional.of(collectorRegistry));
			when(collectorRegistry.getRegisteredEntityTypes()).thenReturn(Set.of());

			CollectionPlannerImpl planner = newPlanner(providerRegistry);

			assertThatThrownBy(() -> planner.plan(PROVIDER_ID, collectorContext()))
					.isInstanceOf(IllegalStateException.class)
					.hasMessageContaining("No collectors registered for provider '" + PROVIDER_ID + "'");
		}

		@Test
		void givenRegisteredCollectorsExist_whenPlanCalled_thenAllRegisteredTypesArePlanned()
				throws PlanningException, CircularReferenceException
		{
			ProviderRegistry providerRegistry = mock(ProviderRegistry.class);
			CollectorRegistryImpl collectorRegistry = mock(CollectorRegistryImpl.class);

			when(providerRegistry.getLoadedCollectorRegistry(PROVIDER_ID)).thenReturn(Optional.of(collectorRegistry));
			when(collectorRegistry.getRegisteredEntityTypes()).thenReturn(Set.of("vpc", "subnet"));
			when(collectorRegistry.getExecutionOrder("vpc")).thenReturn(List.of("vpc"));
			when(collectorRegistry.getExecutionOrder("subnet")).thenReturn(List.of("vpc", "subnet"));

			CollectionPlannerImpl planner = newPlanner(providerRegistry);

			CollectionPlan collectionPlan = planner.plan(PROVIDER_ID, collectorContext());

			assertThat(collectionPlan.getProviderId()).isEqualTo(PROVIDER_ID);
			assertThat(collectionPlan.getCollectorContext()).isNotNull();
			assertThat(collectionPlan.getTargetEntityTypes()).containsExactlyInAnyOrder("vpc", "subnet");
			assertThat(collectionPlan.getSkippedEntityTypes()).isNull();
			assertThat(collectionPlan.getEntityTypeExecutionOrder()).containsExactly("vpc", "subnet");

			verify(collectorRegistry, times(1)).getRegisteredEntityTypes();
			verify(collectorRegistry, never()).getKnownEntityTypesComplement(Set.of("vpc", "subnet"));
		}
	}
}