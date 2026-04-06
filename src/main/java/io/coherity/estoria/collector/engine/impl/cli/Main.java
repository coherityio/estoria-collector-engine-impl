package io.coherity.estoria.collector.engine.impl.cli;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

import io.coherity.estoria.collector.engine.api.CollectionExecutor;
import io.coherity.estoria.collector.engine.api.CollectionPlan;
import io.coherity.estoria.collector.engine.api.CollectionPlanner;
import io.coherity.estoria.collector.engine.api.CollectionResult;
import io.coherity.estoria.collector.engine.api.CollectionRun;
import io.coherity.estoria.collector.engine.api.CollectionRunSummary;
import io.coherity.estoria.collector.engine.api.CollectorEngine;
import io.coherity.estoria.collector.engine.api.SnapshotBuilder;
import io.coherity.estoria.collector.engine.impl.dao.CollectedEntityH2Dao;
import io.coherity.estoria.collector.engine.impl.dao.CollectionResultEntity;
import io.coherity.estoria.collector.engine.impl.dao.CollectionResultH2Dao;
import io.coherity.estoria.collector.engine.impl.dao.CollectionRunEntity;
import io.coherity.estoria.collector.engine.impl.dao.CollectionRunH2Dao;
import io.coherity.estoria.collector.engine.impl.domain.DaoBackedCollectionResult;
import io.coherity.estoria.collector.engine.impl.util.JsonSupport;
import io.coherity.estoria.collector.spi.CloudProvider;
import io.coherity.estoria.collector.spi.Collector;
import io.coherity.estoria.collector.spi.CollectorContext;
import io.coherity.estoria.collector.spi.ProviderContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Main
{
	public static void main(String[] args)
	{
		int exit = new Main().run(args);
		System.exit(exit);
	}

	int run(String[] args)
	{
		if (args == null || args.length == 0)
		{
			printGlobalHelp();
			return 1;
		}

		String command = args[0];
		String[] commandArgs = new String[args.length - 1];
		System.arraycopy(args, 1, commandArgs, 0, commandArgs.length);

		try
		{
			return switch (command)
			{
				case "plan" -> new PlanCommand().run(commandArgs);
				case "execute" -> new ExecuteCommand().run(commandArgs);
				case "snapshot" -> new SnapshotCommand().run(commandArgs);
				case "providers" -> new ProvidersCommand().run(commandArgs);
				case "collectors" -> new CollectorsCommand().run(commandArgs);
				case "-h", "--help", "help" -> {
					printGlobalHelp();
					yield 0;
				}
				default -> {
					System.err.println("Unknown command: " + command);
					printGlobalHelp();
					yield 1;
				}
			};
		}
		catch (ParseException e)
		{
			System.err.println("Failed to parse command line: " + e.getMessage());
			return 2;
		}
		catch (Exception e)
		{
			log.error("Command execution failed", e);
			return 3;
		}
	}

	private void printGlobalHelp()
	{
		System.out.println("""
			Usage: collector <command> [options]
			  Commands:
			    plan      Build a collection plan and optionally write it as JSON.
			    execute   Execute a collection plan from JSON (file or stdin).
			    snapshot  Build a provider snapshot for a given runId.
			    providers List all loaded cloud providers.
			    collectors List collectors for a given provider.

			  Use:
			    collector plan --help
			    collector execute --help
			    collector snapshot --help
			    collector providers
			    collector collectors --help
			  for command-specific options.
			""");
	}

	// Simple DTOs for JSON output
	private record ProviderInfo(String id, String version, String name, Map<String, Object> attributes)
	{
		static ProviderInfo from(CloudProvider provider)
		{
			return new ProviderInfo(
				provider.getId(),
				provider.getVersion(),
				provider.getName(),
				provider.getAttributes());
		}
	}

	private record CollectorInfo(String providerId, String entityType, Set<String> requiresEntityTypes,
		Set<String> tags)
	{
		static CollectorInfo from(Collector collector)
		{
			return new CollectorInfo(
				collector.getProviderId(),
				collector.getEntityType(),
				collector.requiresEntityTypes(),
				collector.getTags());
		}
	}

	@Slf4j
	static class ProvidersCommand
	{
		int run(String[] args) throws Exception
		{
			Options options = new Options();
			options.addOption(Option.builder("h")
				.longOpt("help")
				.desc("Show help")
				.build());

			CommandLineParser parser = new DefaultParser();
			CommandLine cmd = parser.parse(options, args);

			if (cmd.hasOption("help"))
			{
				HelpFormatter hf = new HelpFormatter();
				hf.printHelp("collector providers", options, true);
				return 0;
			}

			CollectorEngine engine = CliUtils.createEngine();
			List<CloudProvider> providers = engine.getLoadedCloudProviders();
			List<ProviderInfo> infos = providers.stream()
				.map(ProviderInfo::from)
				.toList();

			System.out.println(JsonSupport.toJson(infos));
			return 0;
		}
	}

	@Slf4j
	static class CollectorsCommand
	{
		int run(String[] args) throws Exception
		{
			Options options = new Options();
			options.addOption(Option.builder("p")
				.longOpt("provider-id")
				.hasArg()
				.argName("ID")
				.desc("Provider identifier (required)")
				.build());
			options.addOption(Option.builder("h")
				.longOpt("help")
				.desc("Show help")
				.build());

			CommandLineParser parser = new DefaultParser();
			CommandLine cmd = parser.parse(options, args);

			if (cmd.hasOption("help"))
			{
				HelpFormatter hf = new HelpFormatter();
				hf.printHelp("collector collectors", options, true);
				return 0;
			}

			String providerId = cmd.getOptionValue("provider-id");
			if (providerId == null || providerId.isBlank())
			{
				System.err.println("Missing required option: --provider-id");
				HelpFormatter hf = new HelpFormatter();
				hf.printHelp("collector collectors", options, true);
				return 1;
			}

			CollectorEngine engine = CliUtils.createEngine();
			List<Collector> collectors = engine.getLoadedCollectors(providerId);
			List<CollectorInfo> infos = collectors.stream()
				.map(CollectorInfo::from)
				.toList();

			System.out.println(JsonSupport.toJson(infos));
			return 0;
		}
	}

	@Slf4j
	static class PlanCommand
	{
		int run(String[] args) throws Exception
		{
			Options options = new Options();
			options.addOption(Option.builder("p")
				.longOpt("provider-id")
				.hasArg()
				.argName("ID")
				.desc("Provider identifier (required)")
				.build());
			options.addOption(Option.builder("ccf")
				.longOpt("collector-context-file")
				.hasArg()
				.argName("CCFILE")
				.desc("Path to JSON file containing CollectorContext")
				.build());
			options.addOption(Option.builder()
				.longOpt("collector-arg")
				.hasArgs()
				.valueSeparator('=')
				.argName("key=value")
				.desc("Collector argument as key=value (may be repeated)")
				.build());
			options.addOption(Option.builder("t")
				.longOpt("target-types")
				.hasArg()
				.argName("LIST")
				.desc("Comma-separated list of target entity types")
				.build());
			options.addOption(Option.builder("k")
				.longOpt("skip-types")
				.hasArg()
				.argName("LIST")
				.desc("Comma-separated list of entity types to skip")
				.build());
			options.addOption(Option.builder()
				.longOpt("skip-all-dependencies")
				.desc("Skip all dependency entity types")
				.build());
			options.addOption(Option.builder("o")
				.longOpt("output")
				.hasArg()
				.argName("FILE")
				.desc("Write plan JSON to file (default: stdout)")
				.build());
			options.addOption(Option.builder("h")
				.longOpt("help")
				.desc("Show help")
				.build());

			CommandLineParser parser = new DefaultParser();
			CommandLine cmd = parser.parse(options, args);

			if (cmd.hasOption("help"))
			{
				HelpFormatter hf = new HelpFormatter();
				hf.printHelp("collector plan", options, true);
				return 0;
			}

			String providerId = cmd.getOptionValue("provider-id");
			if (providerId == null || providerId.isBlank())
			{
				System.err.println("Missing required option: --provider-id");
				HelpFormatter hf = new HelpFormatter();
				hf.printHelp("collector plan", options, true);
				return 1;
			}
			String collectorContextFile = cmd.getOptionValue("collector-context-file");
			String targetTypesOpt = cmd.getOptionValue("target-types");
			String skipTypesOpt = cmd.getOptionValue("skip-types");
			boolean skipAllDeps = cmd.hasOption("skip-all-dependencies");
			String outputFile = cmd.getOptionValue("output");

			CollectorEngine engine = CliUtils.createEngine();
			CollectionPlanner planner = engine.getPlanner();
			//CloudProvider provider = CliUtils.loadProvider(providerId);

			CollectorContext collectorContext = CliUtils.readJsonIfPresent(collectorContextFile, CollectorContext.class);
			if (collectorContext == null)
			{
				collectorContext = CollectorContext.builder().build();
			}

//			if (scope == null)
//			{
//				scope = CollectionScope.builder()
//					.provider(provider)
//					.build();
//			}
//			else if (scope.getProvider() == null)
//			{
//				scope = CollectionScope.builder()
//					.provider(provider)
//					.attributes(scope.getAttributes())
//					.build();
//			}
			
			
			// Overlay collector-arg key=value pairs into scope attributes when present
			String[] collectorArgs = cmd.getOptionValues("collector-arg");
			if (collectorArgs != null && collectorArgs.length > 0)
			{
				collectorContext = CliCollectorContextFactory.overlayCollectorArgs(collectorContext, collectorArgs);
			}
			Set<String> targetTypes = CliUtils.parseCsvToSet(targetTypesOpt);
			Set<String> skipTypes = CliUtils.parseCsvToSet(skipTypesOpt);

			CollectionPlan plan;

			if (targetTypes != null && !targetTypes.isEmpty())
			{
				if (skipAllDeps)
				{
					plan = planner.plan(providerId, targetTypes, true, collectorContext);
				}
				else if (skipTypes != null && !skipTypes.isEmpty())
				{
					plan = planner.plan(providerId, targetTypes, skipTypes, collectorContext);
				}
				else
				{
					plan = planner.plan(providerId, targetTypes, false, collectorContext);
				}
			}
			else
			{
				if (skipAllDeps)
				{
					System.err.println("Option --skip-all-dependencies requires --target-types to be specified");
					return 1;
				}
				if (skipTypes != null && !skipTypes.isEmpty())
				{
					System.err.println("Option --skip-types requires --target-types to be specified");
					return 1;
				}
				else
				{
					// No target or skip options: let the planner decide the full plan
					plan = planner.plan(providerId, collectorContext);
				}
			}

			if (outputFile != null)
			{
				JsonSupport.writeJsonFile(outputFile, plan);
			}
			else
			{
				System.out.println(JsonSupport.toJson(plan));
			}

			return 0;
		}
	}

	@Slf4j
	static class ExecuteCommand
	{
		int run(String[] args) throws Exception
		{
			Options options = new Options();
			options.addOption(Option.builder("p")
				.longOpt("provider-id")
				.hasArg()
				.argName("ID")
				.desc("Provider identifier (required, must match plan.providerId)")
				.build());
			options.addOption(Option.builder("pcf")
				.longOpt("provider-context-file")
				.hasArg()
				.argName("PCFILE")
				.desc("Path to JSON file containing ProviderContext (credentials, config, etc.)")
				.build());
			options.addOption(Option.builder()
				.longOpt("provider-arg")
				.hasArgs()
				.valueSeparator('=')
				.argName("key=value")
				.desc("provider argument as key=value (may be repeated)")
				.build());
			options.addOption(Option.builder("f")
				.longOpt("plan-file")
				.hasArg()
				.argName("FILE")
				.desc("Path to JSON file containing CollectionPlan")
				.build());
			options.addOption(Option.builder()
				.longOpt("plan-stdin")
				.desc("Read CollectionPlan JSON from stdin")
				.build());
//			options.addOption(Option.builder("c")
//				.longOpt("context-file")
//				.hasArg()
//				.argName("FILE")
//				.desc("Path to JSON file containing ProviderContext (credentials, config, etc.)")
//				.build());
			options.addOption(Option.builder("h")
				.longOpt("help")
				.desc("Show help")
				.build());

			CommandLineParser parser = new DefaultParser();
			CommandLine cmd = parser.parse(options, args);

			if (cmd.hasOption("help"))
			{
				HelpFormatter hf = new HelpFormatter();
				hf.printHelp("collector execute", options, true);
				return 0;
			}

			String providerId = cmd.getOptionValue("provider-id");
			if (providerId == null || providerId.isBlank())
			{
				System.err.println("Missing required option: --provider-id");
				HelpFormatter hf = new HelpFormatter();
				hf.printHelp("collector execute", options, true);
				return 1;
			}
			
			String providerContextFile = cmd.getOptionValue("provider-context-file");
			ProviderContext providerContext = CliUtils.readJsonIfPresent(providerContextFile, ProviderContext.class);
			if (providerContext == null)
			{
				providerContext = ProviderContext.builder().build();
			}
			else if(StringUtils.isNotEmpty(providerId))
			{
				providerContext = ProviderContext.builder().providerId(providerId).build();
			}

			// Overlay provider-arg key=value pairs into provider context attributes when present
			String[] providerArgs = cmd.getOptionValues("provider-arg");
			if (providerArgs != null && providerArgs.length > 0)
			{
				providerContext = CliProviderContextFactory.overlayProviderArgs(providerContext, providerArgs);
			}				
			
			String planFile = cmd.getOptionValue("plan-file");
			boolean planFromStdin = cmd.hasOption("plan-stdin");
			String contextFile = cmd.getOptionValue("context-file");

			if ((planFile == null && !planFromStdin) || (planFile != null && planFromStdin))
			{
				System.err.println("Exactly one of --plan-file or --plan-stdin must be specified.");
				return 1;
			}

			String planJson;
			if (planFromStdin)
			{
				planJson = CliUtils.readAllFromStdin();
			}
			else
			{
				planJson = CliUtils.readAllFromFile(planFile);
			}

			CollectionPlan plan = JsonSupport.fromJson(planJson, CollectionPlan.class);

			if (!providerId.equals(plan.getProviderId()))
			{
				System.err.println("Provider id from CLI (" + providerId
					+ ") does not match plan (" + plan.getProviderId() + ")");
				return 1;
			}

			CollectorEngine engine = CliUtils.createEngine();
			CollectionExecutor executor = engine.getExecutor();

			CollectionRun run = executor.collect(plan, providerContext);

			CollectionRunSummary summary = run.getCollectionRunSummary();
			System.out.println(JsonSupport.toJson(summary));

			return 0;
		}
	}

	@Slf4j
	static class SnapshotCommand
	{
		int run(String[] args) throws IOException
		{
			Options options = new Options();
			options.addOption(Option.builder("r")
				.longOpt("run-id")
				.hasArg()
				.argName("ID")
				.desc("Run identifier to snapshot (required)")
				.required()
				.build());
			options.addOption(Option.builder("h")
				.longOpt("help")
				.desc("Show help")
				.build());

			CommandLineParser parser = new DefaultParser();
			CommandLine cmd;
			try
			{
				cmd = parser.parse(options, args);
			}
			catch (ParseException e)
			{
				HelpFormatter hf = new HelpFormatter();
				hf.printHelp("collector snapshot", options, true);
				System.err.println("Failed to parse snapshot command: " + e.getMessage());
				return 1;
			}

			if (cmd.hasOption("help"))
			{
				HelpFormatter hf = new HelpFormatter();
				hf.printHelp("collector snapshot", options, true);
				return 0;
			}

			String runId = cmd.getOptionValue("run-id");

			CollectionRunH2Dao runDao = new CollectionRunH2Dao();
			CollectionResultH2Dao resultDao = new CollectionResultH2Dao();
			CollectedEntityH2Dao entityDao = new CollectedEntityH2Dao();

			Optional<CollectionRunEntity> runEntityOpt = runDao.findById(runId);
			if (runEntityOpt.isEmpty())
			{
				System.err.println("No collection run found for runId: " + runId);
				return 1;
			}

			CollectionRunEntity runEntity = runEntityOpt.get();
			String providerId = runEntity.getProviderId();

			List<CollectionResultEntity> resultEntities = resultDao.findByRunId(runId);
			if (resultEntities.isEmpty())
			{
				System.err.println("No collection results found for runId: " + runId);
				return 1;
			}

			CollectorEngine engine = CliUtils.createEngine();
			SnapshotBuilder snapshotBuilder = engine.getSnapshotBuilder();

			CliProviderSnapshot providerSnapshot = new CliProviderSnapshot(providerId, null, null);

			resultEntities.forEach(resultEntity -> {
				CollectionResult daoBackedResult = new DaoBackedCollectionResult(resultEntity, entityDao);
				try
				{
					snapshotBuilder.mergeSnapshot(providerSnapshot, daoBackedResult);
				}
				catch (Exception e)
				{
					throw new RuntimeException("Failed to build snapshot for resultId="
						+ resultEntity.getResultId(), e);
				}
			});

			return 0;
		}
	}
}