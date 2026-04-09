package io.coherity.estoria.collector.engine.impl.cli;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import io.coherity.estoria.collector.engine.api.CloudEntityPage;
import io.coherity.estoria.collector.engine.api.CollectionExecutor;
import io.coherity.estoria.collector.engine.api.CollectionFailure;
import io.coherity.estoria.collector.engine.api.CollectionPlan;
import io.coherity.estoria.collector.engine.api.CollectionPlanner;
import io.coherity.estoria.collector.engine.api.CollectionResult;
import io.coherity.estoria.collector.engine.api.CollectionRun;
import io.coherity.estoria.collector.engine.api.CollectionRunFailure;
import io.coherity.estoria.collector.engine.api.CollectionRunSummary;
import io.coherity.estoria.collector.engine.api.CollectionSummary;
import io.coherity.estoria.collector.engine.api.CollectorEngine;
import io.coherity.estoria.collector.engine.api.SnapshotBuilder;
import io.coherity.estoria.collector.engine.impl.dao.CollectedEntityH2Dao;
import io.coherity.estoria.collector.engine.impl.dao.CollectionResultEntity;
import io.coherity.estoria.collector.engine.impl.dao.CollectionResultH2Dao;
import io.coherity.estoria.collector.engine.impl.dao.CollectionRunEntity;
import io.coherity.estoria.collector.engine.impl.dao.CollectionRunH2Dao;
import io.coherity.estoria.collector.engine.impl.domain.DaoBackedCollectionResult;
import io.coherity.estoria.collector.engine.impl.util.JsonSupport;
import io.coherity.estoria.collector.spi.CloudEntity;
import io.coherity.estoria.collector.spi.CloudProvider;
import io.coherity.estoria.collector.spi.Collector;
import io.coherity.estoria.collector.spi.CollectorContext;
import io.coherity.estoria.collector.spi.CollectorInfo;
import io.coherity.estoria.collector.spi.CollectorRegistry;
import io.coherity.estoria.collector.spi.ProviderContext;
import io.coherity.estoria.collector.spi.ProviderInfo;
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
                case "plan"       -> new PlanCommand().run(commandArgs);
                case "collect"    -> new CollectCommand().run(commandArgs);
                case "collection" -> new CollectionCommand().run(commandArgs);
                case "snapshot"   -> new SnapshotCommand().run(commandArgs);
                case "providers"  -> new ProvidersCommand().run(commandArgs);
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
            Usage: estoria-collector <command> [options]
              Commands:
                plan        Build a collection plan and optionally write it as JSON.
                collect     Execute a collection plan from JSON (file or stdin).
                collection  Fetch and stream a completed collection run by run-id.
                snapshot    Build a provider snapshot for a given runId.
                providers   List all loaded cloud providers.
                collectors  List collectors for a given provider.

              Use:
                estoria-collector plan --help
                estoria-collector collect --help
                estoria-collector collection --help
                estoria-collector snapshot --help
                estoria-collector providers
                estoria-collector collectors --help
              for command-specific options.
            """);
    }

    // -------------------------------------------------------------------------
    // providers
    // -------------------------------------------------------------------------

    @Slf4j
    static class ProvidersCommand
    {
        int run(String[] args) throws Exception
        {
            Options options = new Options();
            options.addOption(Option.builder("h").longOpt("help").desc("Show help").build());

            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("help"))
            {
                new HelpFormatter().printHelp("collector providers", options, true);
                return 0;
            }

            CollectorEngine engine = CliUtils.createEngine();
            Set<CloudProvider> providers = engine.getLoadedCloudProviders();
            List<ProviderInfo> infos = providers.stream().map(CloudProvider::getProviderInfo).toList();

            System.out.println(JsonSupport.toJson(infos));
            return 0;
        }
    }

    // -------------------------------------------------------------------------
    // collectors
    // -------------------------------------------------------------------------

    @Slf4j
    static class CollectorsCommand
    {
        int run(String[] args) throws Exception
        {
            Options options = new Options();
            options.addOption(Option.builder("p")
                .longOpt("provider-id").hasArg().argName("ID")
                .desc("Provider identifier (required)").build());
            options.addOption(Option.builder("h").longOpt("help").desc("Show help").build());

            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("help"))
            {
                new HelpFormatter().printHelp("collector collectors", options, true);
                return 0;
            }

            String providerId = cmd.getOptionValue("provider-id");
            if (providerId == null || providerId.isBlank())
            {
                System.err.println("Missing required option: --provider-id");
                new HelpFormatter().printHelp("collector collectors", options, true);
                return 1;
            }

            CollectorEngine engine = CliUtils.createEngine();
            Optional<CloudProvider> opCloudProvider = engine.getLoadedCloudProvider(providerId);
            if (opCloudProvider.isEmpty())
            {
                System.err.println("Provider not found: " + providerId);
                return 1;
            }

            CollectorRegistry collectorRegistry = opCloudProvider.get().getCollectorRegistry();
            if (collectorRegistry == null)
            {
                System.err.println("Collector registry not initialized for provider: " + providerId);
                return 1;
            }

            Set<Collector> collectors = collectorRegistry.getRegisteredCollectors();
            List<CollectorInfo> infos = collectors.stream().map(Collector::getCollectorInfo).toList();

            System.out.println(JsonSupport.toJson(infos));
            return 0;
        }
    }

    // -------------------------------------------------------------------------
    // plan
    // -------------------------------------------------------------------------

    @Slf4j
    static class PlanCommand
    {
        int run(String[] args) throws Exception
        {
            Options options = new Options();
            options.addOption(Option.builder("p")
                .longOpt("provider-id").hasArg().argName("ID")
                .desc("Provider identifier (required)").build());
            options.addOption(Option.builder("ccf")
                .longOpt("collector-context-file").hasArg().argName("CCFILE")
                .desc("Path to JSON file containing CollectorContext").build());
            options.addOption(Option.builder()
                .longOpt("collector-arg").hasArgs().valueSeparator('=').argName("key=value")
                .desc("Collector argument as key=value (may be repeated)").build());
            options.addOption(Option.builder("t")
                .longOpt("target-types").hasArg().argName("LIST")
                .desc("Comma-separated list of target entity types").build());
            options.addOption(Option.builder("k")
                .longOpt("skip-types").hasArg().argName("LIST")
                .desc("Comma-separated list of entity types to skip").build());
            options.addOption(Option.builder()
                .longOpt("skip-all-dependencies")
                .desc("Skip all dependency entity types").build());
            options.addOption(Option.builder("o")
                .longOpt("output").hasArg().argName("FILE")
                .desc("Write plan JSON to file (default: stdout)").build());
            options.addOption(Option.builder("h").longOpt("help").desc("Show help").build());

            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("help"))
            {
                new HelpFormatter().printHelp("collector plan", options, true);
                return 0;
            }

            String providerId = cmd.getOptionValue("provider-id");
            if (providerId == null || providerId.isBlank())
            {
                System.err.println("Missing required option: --provider-id");
                new HelpFormatter().printHelp("collector plan", options, true);
                return 1;
            }

            String collectorContextFile = cmd.getOptionValue("collector-context-file");
            String targetTypesOpt       = cmd.getOptionValue("target-types");
            String skipTypesOpt         = cmd.getOptionValue("skip-types");
            boolean skipAllDeps         = cmd.hasOption("skip-all-dependencies");
            String outputFile           = cmd.getOptionValue("output");

            CollectorEngine engine = CliUtils.createEngine();
            CollectionPlanner planner = engine.getPlanner();

            CollectorContext collectorContext =
                CliUtils.readJsonIfPresent(collectorContextFile, CollectorContext.class);
            if (collectorContext == null)
            {
                collectorContext = CollectorContext.builder().build();
            }

            String[] collectorArgs = cmd.getOptionValues("collector-arg");
            if (collectorArgs != null && collectorArgs.length > 0)
            {
                collectorContext =
                    CliCollectorContextFactory.overlayCollectorArgs(collectorContext, collectorArgs);
            }

            Set<String> targetTypes = CliUtils.parseCsvToSet(targetTypesOpt);
            Set<String> skipTypes   = CliUtils.parseCsvToSet(skipTypesOpt);

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
                    System.err.println("--skip-all-dependencies requires --target-types");
                    return 1;
                }
                if (skipTypes != null && !skipTypes.isEmpty())
                {
                    System.err.println("--skip-types requires --target-types");
                    return 1;
                }
                plan = planner.plan(providerId, collectorContext);
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

    // -------------------------------------------------------------------------
    // collect
    // -------------------------------------------------------------------------

    @Slf4j
    static class CollectCommand
    {
        int run(String[] args) throws Exception
        {
            Options options = new Options();
            options.addOption(Option.builder("p")
                .longOpt("provider-id").hasArg().argName("ID")
                .desc("Provider identifier (required, must match plan.providerId)").build());
            options.addOption(Option.builder("pcf")
                .longOpt("provider-context-file").hasArg().argName("PCFILE")
                .desc("Path to JSON file containing ProviderContext").build());
            options.addOption(Option.builder()
                .longOpt("provider-arg").hasArgs().valueSeparator('=').argName("key=value")
                .desc("Provider argument as key=value (may be repeated)").build());
            options.addOption(Option.builder("f")
                .longOpt("plan-file").hasArg().argName("FILE")
                .desc("Path to JSON file containing CollectionPlan").build());
            options.addOption(Option.builder()
                .longOpt("plan-stdin")
                .desc("Read CollectionPlan JSON from stdin").build());
            options.addOption(Option.builder("h").longOpt("help").desc("Show help").build());

            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("help"))
            {
                new HelpFormatter().printHelp("collector collect", options, true);
                return 0;
            }

            String providerId = cmd.getOptionValue("provider-id");
            if (providerId == null || providerId.isBlank())
            {
                System.err.println("Missing required option: --provider-id");
                new HelpFormatter().printHelp("collector collect", options, true);
                return 1;
            }

            String providerContextFile = cmd.getOptionValue("provider-context-file");
            ProviderContext providerContext =
                CliUtils.readJsonIfPresent(providerContextFile, ProviderContext.class);
            if (providerContext == null)
            {
                providerContext = ProviderContext.builder().attributes(new HashMap<>()).build();
            }

            Properties providerArgs = cmd.getOptionProperties("provider-arg");
            if (providerArgs != null && !providerArgs.isEmpty())
            {
                providerContext =
                    CliProviderContextFactory.overlayProviderArgs(providerContext, providerArgs);
            }

            String planFile       = cmd.getOptionValue("plan-file");
            boolean planFromStdin = cmd.hasOption("plan-stdin");

            if ((planFile == null && !planFromStdin) || (planFile != null && planFromStdin))
            {
                System.err.println("Exactly one of --plan-file or --plan-stdin must be specified.");
                return 1;
            }

            String planJson = planFromStdin
                ? CliUtils.readAllFromStdin()
                : CliUtils.readAllFromFile(planFile);

            CollectionPlan plan = JsonSupport.fromJson(planJson, CollectionPlan.class);

            if (!providerId.equals(plan.getProviderId()))
            {
                System.err.println("--provider-id (" + providerId
                    + ") does not match plan.providerId (" + plan.getProviderId() + ")");
                return 1;
            }

            CollectorEngine engine   = CliUtils.createEngine();
            CollectionExecutor executor = engine.getExecutor();
            CollectionRun run = executor.collect(plan, providerContext);

            // CollectionRunView deliberately omits getCollectionResults() to
            // avoid triggering additional DB reads after execution.
            System.out.println(JsonSupport.toPrettyJson(CollectionRunView.from(run)));

            return 0;
        }
    }

    // -------------------------------------------------------------------------
    // collection  (fetch + stream a completed run by run-id)
    // -------------------------------------------------------------------------

    @Slf4j
    static class CollectionCommand
    {
        private static final int DEFAULT_PAGE_SIZE = 100;

        int run(String[] args) throws Exception
        {
            Options options = new Options();
            options.addOption(
            		Option.builder("r")
	            		.longOpt("run-id")
	            		.hasArg()
	            		.argName("ID")
	            		.desc("Run identifier to retrieve (required)")
	            		.build());
            options.addOption(
            		Option.builder("s")
	            		.longOpt("page-size")
	            		.hasArg()
	            		.argName("N")
	            		.desc("Number of entities to fetch per page (default: " + DEFAULT_PAGE_SIZE + ")")
	            		.build());
            options.addOption(
            		Option.builder("o")
	            		.longOpt("output")
	            		.hasArg()
	            		.argName("FILE")
	            		.desc("Write output to file instead of stdout")
	            		.build());
            options.addOption(
            		Option.builder("h")
	            		.longOpt("help")
	            		.desc("Show help")
	            		.build());
            options.addOption(
            	    Option.builder()
            	        .longOpt("pretty")
            	        .desc("Pretty-print JSON output (2-space indentation)")
            	        .build());            

            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);

            if (cmd.hasOption("help"))
            {
                new HelpFormatter().printHelp("collector collection", options, true);
                return 0;
            }

            String runId = cmd.getOptionValue("run-id");
            if (runId == null || runId.isBlank())
            {
                System.err.println("Missing required option: --run-id");
                new HelpFormatter().printHelp("collector collection", options, true);
                return 1;
            }

            int pageSize = DEFAULT_PAGE_SIZE;
            String pageSizeOpt = cmd.getOptionValue("page-size");
            if (pageSizeOpt != null)
            {
                try
                {
                    pageSize = Integer.parseInt(pageSizeOpt);
                    if (pageSize < 1)
                    {
                        System.err.println("--page-size must be a positive integer.");
                        return 1;
                    }
                }
                catch (NumberFormatException e)
                {
                    System.err.println("--page-size must be a valid integer, got: " + pageSizeOpt);
                    return 1;
                }
            }

            String outputFile = cmd.getOptionValue("output");
            boolean prettyFormat = cmd.hasOption("pretty");
            
            CollectorEngine engine = CliUtils.createEngine();
            CollectionExecutor executor = engine.getExecutor();
            CollectionRun run = executor.getCollectedRun(runId);

            if (run == null)
            {
                System.err.println("No collection run found for runId: " + runId);
                return 1;
            }

            // getCollectionResults() is intentional here — this command's entire
            // purpose is to stream the full run. Pages are flushed immediately.
            List<CollectionResult> results = run.getCollectionResults();
            CliUtils.streamCollectionRun(run, results, pageSize, outputFile, prettyFormat);

            return 0;
        }
    }

    // -------------------------------------------------------------------------
    // snapshot
    // -------------------------------------------------------------------------

    @Slf4j
    static class SnapshotCommand
    {
        int run(String[] args) throws Exception
        {
            Options options = new Options();
            options.addOption(
            		Option.builder("r")
            			.longOpt("run-id")
            			.hasArg()
            			.argName("ID")
            			.desc("Run identifier to snapshot (required)")
            			.build());
            options.addOption(Option.builder("h").longOpt("help").desc("Show help").build());

            CommandLineParser parser = new DefaultParser();
            CommandLine cmd;
            try
            {
                cmd = parser.parse(options, args);
            }
            catch (ParseException e)
            {
                new HelpFormatter().printHelp("collector snapshot", options, true);
                System.err.println("Failed to parse snapshot command: " + e.getMessage());
                return 1;
            }

            if (cmd.hasOption("help"))
            {
                new HelpFormatter().printHelp("collector snapshot", options, true);
                return 0;
            }

            String runId = cmd.getOptionValue("run-id");
            if (runId == null || runId.isBlank())
            {
                System.err.println("Missing required option: --run-id");
                new HelpFormatter().printHelp("collector snapshot", options, true);
                return 1;
            }

            CollectionRunH2Dao runDao = new CollectionRunH2Dao();
            CollectionResultH2Dao resultDao = new CollectionResultH2Dao();
            CollectedEntityH2Dao entityDao = new CollectedEntityH2Dao();

            Optional<CollectionRunEntity> runEntityOpt = runDao.findById(runId);
            if (runEntityOpt.isEmpty())
            {
                System.err.println("No collection run found for runId: " + runId);
                return 1;
            }

            List<CollectionResultEntity> resultEntities = resultDao.findByRunId(runId);
            if (resultEntities == null || resultEntities.isEmpty())
            {
                System.err.println("No collection results found for runId: " + runId);
                return 1;
            }

            CollectorEngine engine = CliUtils.createEngine();
            SnapshotBuilder snapshotBuilder = engine.getSnapshotBuilder();

            CliProviderSnapshot providerSnapshot =
                new CliProviderSnapshot(runEntityOpt.get().getProviderId(), null, null);

            for (CollectionResultEntity resultEntity : resultEntities)
            {
                CollectionResult daoBackedResult =
                    new DaoBackedCollectionResult(resultEntity, entityDao);
                try
                {
                    snapshotBuilder.mergeSnapshot(providerSnapshot, daoBackedResult);
                }
                catch (Exception e)
                {
                    throw new RuntimeException(
                        "Failed to build snapshot for resultId=" + resultEntity.getResultId(), e);
                }
            }

            return 0;
        }
    }
}