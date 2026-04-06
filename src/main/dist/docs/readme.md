# Estoria Collector Engine CLI

This CLI wraps the Estoria Collector Engine and exposes several commands:

- `plan` – build a collection plan for a provider.
- `execute` – execute a collection plan and produce a run summary.
- `snapshot` – rebuild a provider snapshot from stored run results.
- `providers` – list all loaded cloud providers.
- `collectors` – list collectors for a specific provider.

The launcher script is `collector-cli` (Unix) or `collector-cli.bat` (Windows). The distribution ZIP provides a layout:

- `bin/` – wrapper scripts (`collector-cli`, `collector-cli.bat`).
- `lib/` – application JAR and all dependencies.
- `conf/` – configuration files (e.g. `log4j2.xml`).
- `docs/` – this documentation.

---

## Global Usage

```bash
collector-cli <command> [options]
```

Commands:

- `plan` – build and optionally write a `CollectionPlan` as JSON.
- `execute` – run a previously generated `CollectionPlan`.
- `snapshot` – build a provider snapshot for a given run id.
- `providers` – list loaded cloud providers as JSON.
- `collectors` – list collectors for a given provider as JSON.

For command-specific help:

```bash
collector-cli plan --help
collector-cli execute --help
collector-cli snapshot --help
collector-cli providers
collector-cli collectors --help
```

On Windows PowerShell:

```powershell
./collector-cli.bat plan --help
./collector-cli.bat execute --help
./collector-cli.bat snapshot --help
./collector-cli.bat providers
./collector-cli.bat collectors --help
```

---

## `plan` Command

Builds a collection plan for a specific provider and scope.

**Options:**

- `-p, --provider-id <ID>` (required) – provider identifier.
- `-s, --scope-file <FILE>` – JSON file containing a `CollectionScope`.
- `--provider-arg key=value` – additional scope/provider arguments (repeatable).
- `-t, --target-types <LIST>` – comma-separated list of target entity types.
- `-k, --skip-types <LIST>` – comma-separated list of entity types to skip.
- `--skip-all-dependencies` – skip all dependency entity types.
- `-o, --output <FILE>` – write plan JSON to a file (default: stdout).
- `-h, --help` – show command help.

**Typical JSON scope file** (`scope.json`):

```json
{
  "provider": "aws",
  "attributes": {
    "region": "us-east-1"
  }
}
```

**Example – Unix (bash/zsh):**

```bash
# Plan for AWS EC2 instances in a single region, writing to plan.json
./collector-cli plan \
  --provider-id aws \
  --scope-file scope.json \
  --provider-arg profile=dev \
  --target-types EC2Instance \
  --output plan.json
```

**Example – Windows PowerShell:**

```powershell
# Plan for AWS EC2 instances in a single region, writing to plan.json
./collector-cli.bat plan `
  --provider-id aws `
  --scope-file scope.json `
  --provider-arg profile=dev `
  --target-types EC2Instance `
  --output plan.json
```

**Using multiple `--provider-arg` values:**

```bash
./collector-cli plan \
  --provider-id aws \
  --provider-arg profile=dev \
  --provider-arg region=us-east-1 \
  --output plan.json
```

---

## `execute` Command

Executes a collection plan and writes a `CollectionRunSummary` as JSON.

**Options:**

- `-p, --provider-id <ID>` (required) – must match `plan.providerId`.
- `-f, --plan-file <FILE>` – JSON file containing a `CollectionPlan`.
- `--plan-stdin` – read `CollectionPlan` JSON from stdin.
- `-c, --context-file <FILE>` – JSON file containing `ProviderContext` (credentials, config).
- `-h, --help` – show command help.

Exactly one of `--plan-file` or `--plan-stdin` must be specified.

**Example – Execute from plan file (Unix):**

```bash
./collector-cli execute \
  --provider-id aws \
  --plan-file plan.json \
  --context-file context.json > run-summary.json
```

**Example – Execute from plan file (PowerShell):**

```powershell
./collector-cli.bat execute `
  --provider-id aws `
  --plan-file plan.json `
  --context-file context.json `
  > run-summary.json
```

**Example – Execute using stdin (Unix):**

```bash
cat plan.json | ./collector-cli execute \
  --provider-id aws \
  --plan-stdin \
  --context-file context.json
```

---

## `snapshot` Command

Rebuilds a provider snapshot for an existing collection run using the stored run and result data in the local H2 database.

**Options:**

- `-r, --run-id <ID>` (required) – run identifier to snapshot.
- `-h, --help` – show command help.

The snapshot builder reads all `CollectionResult` records for the specified run id and merges them into a provider snapshot. The exact output (files, directories) is determined by the configured `SnapshotBuilder` implementation.

**Example – Unix:**

```bash
./collector-cli snapshot --run-id 12345
```

**Example – PowerShell:**

```powershell
./collector-cli.bat snapshot --run-id 12345
```

---

## `providers` Command

Lists all loaded cloud providers from the engine as JSON.

Each provider entry has the shape:

```json
{
  "id": "aws",
  "version": "1.0.0",
  "name": "Amazon Web Services",
  "attributes": {
    "someKey": "someValue"
  }
}
```

**Examples – Unix:**

```bash
./collector-cli providers
```

**Examples – PowerShell:**

```powershell
./collector-cli.bat providers
```

---

## `collectors` Command

Lists all collectors for a specific provider as JSON.

**Options:**

- `-p, --provider-id <ID>` (required) – provider identifier.
- `-h, --help` – show command help.

Each collector entry has the shape:

```json
{
  "providerId": "aws",
  "entityType": "EC2Instance",
  "requiresEntityTypes": ["Vpc", "Subnet"],
  "tags": ["aws", "compute"]
}
```

**Example – Unix:**

```bash
./collector-cli collectors --provider-id aws
```

**Example – PowerShell:**

```powershell
./collector-cli.bat collectors --provider-id aws
```

---

## Logging Configuration

The launcher scripts add the `conf/` directory to the Java classpath. To configure Log4j2, place a `log4j2.xml` file in `conf/` (as shipped in this distribution or your customized version). When you run `collector-cli`, Log4j2 will automatically load `conf/log4j2.xml` if present.

---

## Exit Codes (Summary)

- `0` – success.
- `1` – command-specific validation or usage error (e.g., missing required option, bad combination of options).
- `2` – generic command-line parsing error at the top level.
- `3` – unexpected exception during command execution.

Use these exit codes in scripts or automation to detect and handle failures appropriately.
