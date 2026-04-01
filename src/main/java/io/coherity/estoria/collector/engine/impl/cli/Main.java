package io.coherity.estoria.collector.engine.impl.cli;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import io.coherity.estoria.collector.engine.api.CollectionExecutor;
import io.coherity.estoria.collector.engine.api.CollectionPlanner;
import io.coherity.estoria.collector.engine.api.CollectorEngine;
import io.coherity.estoria.collector.engine.api.SnapshotBuilder;
import io.coherity.estoria.collector.engine.api.CollectionPlan;
import io.coherity.estoria.collector.engine.api.CollectionResult;
import io.coherity.estoria.collector.engine.api.CollectionRun;
import io.coherity.estoria.collector.engine.api.CollectionRunSummary;
import io.coherity.estoria.collector.engine.impl.dao.CollectedEntityH2Dao;
import io.coherity.estoria.collector.engine.impl.dao.CollectionResultEntity;
import io.coherity.estoria.collector.engine.impl.dao.CollectionResultH2Dao;
import io.coherity.estoria.collector.engine.impl.dao.CollectionRunEntity;
import io.coherity.estoria.collector.engine.impl.dao.CollectionRunH2Dao;
import io.coherity.estoria.collector.engine.impl.domain.DaoCollectionResult;
import io.coherity.estoria.collector.spi.CollectionScope;
import io.coherity.estoria.collector.spi.CloudProvider;
import io.coherity.estoria.collector.spi.ProviderContext;
import io.coherity.estoria.collector.spi.ProviderSession;
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
			Usage: collector-cli <command> [options]
			  Commands:
			    plan      Build a collection plan and optionally write it as JSON.
			    execute   Execute a collection plan from JSON (file or stdin).
			    snapshot  Build a provider snapshot for a given runId.

			  Use:
			    collector-cli plan --help
			    collector-cli execute --help
			    collector-cli snapshot --help
			  for command-specific options.
			""");
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
				.required()
				.build());
			options.addOption(Option.builder("s")
				.longOpt("scope-file")
				.hasArg()
				.argName("FILE")
				.desc("Path to JSON file containing CollectionScope")
				.build());
			options.addOption(Option.builder()
				.longOpt("provider-arg")
				.hasArgs()
				.valueSeparator('=')
				.argName("key=value")
				.desc("Provider or scope argument as key=value (may be repeated)")
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
				hf.printHelp("collector-cli plan", options, true);
				return 0;
			}

			String providerId = cmd.getOptionValue("provider-id");
			String scopeFile = cmd.getOptionValue("scope-file");
			String targetTypesOpt = cmd.getOptionValue("target-types");
			String skipTypesOpt = cmd.getOptionValue("skip-types");
			boolean skipAllDeps = cmd.hasOption("skip-all-dependencies");
			String outputFile = cmd.getOptionValue("output");

			CollectorEngine engine = CliUtils.createEngine();
			CollectionPlanner planner = engine.getPlanner();
			CloudProvider provider = CliUtils.loadProvider(providerId);

			CollectionScope scope = CliUtils.readJsonIfPresent(scopeFile, CollectionScope.class);
			// Overlay provider-arg key=value pairs into scope attributes when present
			String[] providerArgs = cmd.getOptionValues("provider-arg");
			if (providerArgs != null && providerArgs.length > 0)
			{
				scope = CliScopeFactory.overlayProviderArgs(scope, providerArgs);
			}
			Set<String> targetTypes = CliUtils.parseCsvToSet(targetTypesOpt);
			Set<String> skipTypes = CliUtils.parseCsvToSet(skipTypesOpt);

			CollectionPlan plan;

			if (skipAllDeps)
			{
				plan = planner.plan(provider, scope, targetTypes, true);
			}
			else if (skipTypes != null && !skipTypes.isEmpty())
			{
				plan = planner.plan(provider, scope, targetTypes, skipTypes);
			}
			else if (targetTypes != null && !targetTypes.isEmpty())
			{
				plan = planner.plan(provider, scope, targetTypes, false);
			}
			else
			{
				plan = planner.plan(provider, scope);
			}

			if (outputFile != null)
			{
				JsonUtil.writeJsonFile(outputFile, plan);
			}
			else
			{
				System.out.println(JsonUtil.toJson(plan));
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
				.required()
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
			options.addOption(Option.builder("c")
				.longOpt("context-file")
				.hasArg()
				.argName("FILE")
				.desc("Path to JSON file containing ProviderContext (credentials, config, etc.)")
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
				hf.printHelp("collector-cli execute", options, true);
				return 0;
			}

			String providerId = cmd.getOptionValue("provider-id");
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

			CollectionPlan plan = JsonUtil.fromJson(planJson, CollectionPlan.class);

			if (!providerId.equals(plan.getProviderId()))
			{
				System.err.println("Provider id from CLI (" + providerId
					+ ") does not match plan (" + plan.getProviderId() + ")");
				return 1;
			}

			CollectorEngine engine = CliUtils.createEngine();
			CollectionExecutor executor = engine.getExecutor();

			ProviderContext context = CliUtils.readJsonIfPresent(contextFile, ProviderContext.class);
			ProviderSession session = CliUtils.loadProvider(providerId).openSession(context);

			CollectionRun run = executor.collect(plan, session);

			CollectionRunSummary summary = run.getCollectionRunSummary();
			System.out.println(JsonUtil.toJson(summary));

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
				hf.printHelp("collector-cli snapshot", options, true);
				System.err.println("Failed to parse snapshot command: " + e.getMessage());
				return 1;
			}

			if (cmd.hasOption("help"))
			{
				HelpFormatter hf = new HelpFormatter();
				hf.printHelp("collector-cli snapshot", options, true);
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
				CollectionResult daoBackedResult = new DaoCollectionResult(
					resultEntity.getResultId(), resultDao, entityDao);
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