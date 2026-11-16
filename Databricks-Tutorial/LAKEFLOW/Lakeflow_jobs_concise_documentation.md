# Databricks Lakeflow Jobs --- Concise Documentation

> **Scope:** A practical, end-to-end reference covering the Lakeflow
> Jobs concepts discussed, plus important operational and production
> considerations.\
> **Documentation baseline:** Databricks documentation checked on 16
> November 2026. Some features are cloud-, workspace-, task-, or
> preview-dependent; verify availability in your workspace.

## 1. What is Lakeflow Jobs?

**Lakeflow Jobs** is Databricks' workflow orchestration service. It
coordinates, schedules, executes, and monitors one or more tasks that
together implement a data, analytics, or machine-learning workflow.

-   **Job:** The overall workflow definition and orchestration resource.
-   **Task:** A unit of work within a Job.
-   **Run:** One execution of a Job.
-   **Task run:** One execution of a task within a Job run.
-   **DAG:** A directed acyclic graph representing task dependencies and
    execution order.

Typical uses include ETL/ELT, notebook execution, SQL workloads,
pipeline execution, ML workflows, and coordinating external tools such
as dbt.

## 2. Jobs, Tasks, and Task Types

A Job contains one or more tasks. Tasks can run sequentially or in
parallel, depending on their dependencies.

Common task types include:

  -----------------------------------------------------------------------
  Task type                           Typical purpose
  ----------------------------------- -----------------------------------
  Notebook                            Run a Databricks notebook

  Python script                       Run a Python file

  Python wheel                        Run a packaged Python application

  SQL                                 Execute SQL statements or SQL files

  Pipeline                            Run a Lakeflow Spark Declarative
                                      Pipeline

  Ingestion / Database Table Sync     Run supported managed ingestion or
                                      synchronization workloads

  dbt                                 Run dbt workloads

  JAR / Spark Submit                  Run JVM or Spark applications

  Run Job                             Trigger another Job in the
                                      workspace

  If/else                             Branch based on a boolean condition

  For each                            Repeat a nested task for an array
                                      of inputs

  Dashboard / SQL Alert / Power BI    Run supported analytics and BI
                                      tasks
  -----------------------------------------------------------------------

The task types available depend on workspace configuration, cloud, and
feature release.

### Example: Medallion workflow

``` text
Ingest to Bronze
       |
       v
Transform Silver
       |
       v
Build Gold
       |
       v
Data Quality Check
       |
       v
Notify / Publish
```

Independent source ingestions can run in parallel and converge at a
downstream task.

## 3. Dependencies and Workflow Control

### 3.1 Sequential and parallel execution

-   **Sequential:** A downstream task waits for its upstream dependency.
-   **Parallel:** Independent tasks can execute at the same time.
-   Databricks schedules upstream tasks before downstream tasks and runs
    as many independent tasks in parallel as possible.

### 3.2 Run if --- status-based dependencies

Use **Run if** to decide whether a task should run based on the outcome
of its upstream tasks.

  Condition                Meaning
  ------------------------ -----------------------------------------------------
  All succeeded            Every upstream task succeeded
  At least one succeeded   One or more upstream tasks succeeded
  None failed              No upstream task failed
  All done                 All upstream tasks completed, regardless of outcome
  At least one failed      One or more upstream tasks failed
  All failed               Every upstream task failed

Example: A notification task can use **At least one failed** to run when
one or more upstream tasks fail.

**Important:** Run if evaluates task outcomes; it does not evaluate
arbitrary business expressions such as `record_count > 1000`.

### 3.3 If/else --- value-based branching

Use an **If/else condition** task to evaluate a boolean expression and
branch into true or false paths.

Conditions can reference: - Job parameters. - Task parameters. - Task
values from upstream tasks.

Supported comparison operators:

`==`, `!=`, `>`, `>=`, `<`, `<=`

Example:

``` text
Calculate record_count
          |
          v
 record_count > 1000?
       /       \
     True      False
      |          |
 Load Gold   Send Alert
```

Use If/else for business rules, data-quality thresholds,
environment-specific logic, or deciding whether new data is available.

### 3.4 For each --- iterative execution

A **For each** task loops over an input array and runs one nested task
for each item.

It has two components:

-   **For each container:** Defines the input array and iteration
    concurrency.
-   **Nested task:** The standard task executed for each input item.

Example input:

``` json
["customer", "orders", "products"]
```

The nested task receives the current item using `{{input}}`. For an
array of objects, use `{{input.<key>}}` to access an object field.

Key points: - The nested task is executed once per input item. -
Independent iterations can run concurrently. - Configure concurrency to
control the number of simultaneous iterations. - Downstream dependencies
attach to the For each container, not directly to the nested task. - A
For each task cannot contain another For each task as its nested task.

**Concurrency note:** The supplied workspace screenshot showed UI
validation accepting values from **1 to 4**. Treat that as the limit
displayed by that workspace/UI, not as a universal Databricks limit for
every environment or release. The default is 1. This setting is distinct
from the Job's maximum concurrent runs and workspace-wide task-run
limits.

### 3.5 Disabled tasks

-   A disabled task is skipped at runtime without being deleted.
-   Its configuration and run history remain.
-   Downstream tasks evaluate their Run if conditions to determine
    whether they should run.

### 3.6 Run Job task

-   Runs another Job in the same workspace.
-   Can pass parameters to the called Job.
-   Useful for composing reusable workflows.
-   Avoid circular Job dependencies. Databricks documents a maximum
    nesting depth of three Run Job tasks.

## 4. Parameters and Passing Values

Parameters make Jobs and tasks reusable by allowing input values to vary
between executions.

### 4.1 Job parameters

-   Key-value pairs defined at Job level.
-   Passed to tasks that accept key-value parameters.
-   Can be overridden when starting a run.
-   Useful for shared inputs such as environment, processing date,
    region, or source system.

Example:

``` text
environment = dev
processing_date = 2026-11-16
```

### 4.2 Task parameters

-   Inputs configured for an individual task.
-   Used to pass task-specific values, such as a table name, file path,
    or processing mode.
-   The way parameters are supplied and read depends on the task type.

### 4.3 Accessing parameters

For a Notebook task, use widgets to retrieve parameters:

``` python
environment = dbutils.widgets.get("environment")
table_name = dbutils.widgets.get("table_name")
```

Parameter access differs by task type; use the task-specific Databricks
documentation.

### 4.4 Task values

**Task Values** let code running in one task publish computed values for
other tasks in the same Job run.

Set a value:

``` python
dbutils.jobs.taskValues.set(
    key="record_count",
    value=1000
)
```

Retrieve it in a downstream task:

``` python
record_count = dbutils.jobs.taskValues.get(
    taskKey="validate_data",
    key="record_count"
)
```

Typical uses: - Record counts. - Validation outcomes. - Generated output
paths. - Runtime metadata.

Task Values are runtime outputs, not a replacement for Job or Task input
parameters.

### 4.5 Dynamic value references

Dynamic value references use `{{...}}` syntax in supported Job/task
configuration fields to resolve runtime metadata, parameters, and task
outputs.

Examples:

  -------------------------------------------------------------------------------
  Reference                                   Purpose
  ------------------------------------------- -----------------------------------
  `{{job.run_id}}`                            Current Job run identifier

  `{{job.parameters.environment}}`            Job parameter

  `{{task.name}}`                             Current task name

  `{{task.retry_count}}`                      Current task retry count

  `{{tasks.<task_key>.values.<value_key>}}`   Task Value published by another
                                              task

  `{{input}}`                                 Current For each iteration input
  -------------------------------------------------------------------------------

Use the exact reference names and supported fields from the Dynamic
Value References documentation. Dynamic references are
configuration-time substitutions; Task Values are values explicitly
written by task code.

### 4.6 Quick distinction

  -----------------------------------------------------------------------
  Concept                             Role
  ----------------------------------- -----------------------------------
  Job parameter                       Supplies common workflow input

  Task parameter                      Supplies input to an individual
                                      task

  Task Value                          Publishes a value computed during a
                                      task run

  Dynamic value reference             References supported runtime
                                      metadata, parameters, or task
                                      outputs in configuration
  -----------------------------------------------------------------------

## 5. Triggers and Scheduling

A trigger determines when a Job run starts.

  -----------------------------------------------------------------------
  Trigger                 Behavior                Typical use
  ----------------------- ----------------------- -----------------------
  Scheduled               Runs at configured      Hourly or daily ETL
                          times or intervals      

  File arrival            Runs when new files     File ingestion
                          arrive in a monitored   
                          Unity Catalog storage   
                          location                

  Table update            Runs when monitored     Data-driven
                          supported tables are    orchestration
                          updated                 

  Continuous              Starts another run when Always-on workloads
                          the prior run completes 
                          or fails                

  Manual / None           Started by a user or    Testing, backfills, ad
                          programmatic request    hoc runs

  Upstream Job completion Starts after a          Chained workflows
                          specified upstream Job  
                          completes with a        
                          matching result         

  Model update            Starts on supported     Model lifecycle
                          Unity Catalog model     automation
                          events                  
  -----------------------------------------------------------------------

Trigger availability can vary by workspace and preview status. Multiple
triggers on one Job are a Beta feature in the cited documentation.

### 5.1 Scheduled trigger

-   Runs a Job at specified times or recurring intervals.
-   Configure through the UI or cron schedule.
-   Select an appropriate time zone.
-   Consider daylight-saving behavior when scheduling across regions.

### 5.2 File arrival trigger

-   Monitors a path that is the root or a subpath of a Unity Catalog
    **External Location** or **Volume**.
-   Supported cloud storage includes Amazon S3, Azure Storage, and
    Google Cloud Storage.
-   Checks for new files on a best-effort basis (the documentation
    describes checks approximately every minute).
-   Recursively checks subdirectories.
-   Requires Unity Catalog and appropriate storage permissions; the Job
    identity needs `READ` access.
-   Databricks recommends enabling Managed File Events on the external
    location for scalability and performance.

Important behavior: - New files can trigger a run. - Overwriting an
existing file with the same name does not normally count as a new
arrival. - Wildcards are not supported in the monitored path. - Use Auto
Loader in the triggered Job to incrementally process arriving files; the
trigger starts orchestration, while Auto Loader handles file discovery
and ingestion state.

Advanced controls: - **Minimum time between triggers:** Cooldown that
limits how frequently runs start. - **Wait after last change:** Debounce
interval; waits until file arrivals pause before starting a run.

### 5.3 Table update trigger

-   Monitors supported Unity Catalog tables and triggers a Job when data
    changes.
-   Can trigger when **Any table** or **All tables** in the configured
    list are updated.
-   Supports Unity Catalog managed Delta and Iceberg tables, Unity
    Catalog external tables backed by Delta Lake, materialized views,
    streaming tables, and supported views/metric views that depend on
    supported tables.
-   The documentation allows up to 10 tables per trigger in the
    configuration described in the learning material; check current
    workspace UI/docs for current limits.

**Important distinction:** A Unity Catalog external table backed by
Delta Lake is not the same as a foreign table in an external database. A
SQL Server table queried through JDBC or Lakehouse Federation is not
directly made eligible for a Table Update Trigger merely by being
queryable in Databricks. Ingest or replicate its changes into a
supported Unity Catalog table, or use external change
detection/orchestration and call the Jobs API.

### 5.4 Continuous trigger

-   Keeps a Job running by starting a new run when the previous run
    completes or fails.
-   Databricks applies exponential backoff when continuous runs fail.
-   Commonly used for always-on streaming workloads.
-   For streaming workloads, use appropriate checkpointing and
    understand the compute/task-specific limitations.

### 5.5 Manual and programmatic runs

A Job can be started from the UI (Run now), or programmatically through
supported APIs, CLI, SDKs, or deployment workflows. Manual runs are
useful for testing, backfills, and one-off processing.

### 5.6 Job concurrency and queueing

Do not confuse these separate controls:

-   **Maximum concurrent runs:** How many runs of the same Job can be
    active simultaneously. The default is generally 1.
-   **For each concurrency:** How many iterations of one For each task
    can run at once.
-   **Workspace task-run limits:** Workspace-wide service limits, not
    the same as either Job setting.

When a trigger fires while a Job is already at its concurrency limit,
behavior depends on trigger type and queue configuration. Runs can be
skipped or queued according to the Job configuration and trigger
behavior. Enable queueing when retaining eligible runs is important.

## 6. Retries, Timeouts, and Failure Recovery

### 6.1 Retry policy

-   Specifies how many times a failed task is attempted again.
-   Helps recover from transient failures such as temporary network or
    service interruptions.
-   Not every Job/task configuration supports retries.
-   Continuous Jobs use automatic retry behavior with exponential
    backoff.
-   Serverless Jobs can have automatic retry optimization enabled by
    default, depending on configuration.

Design retries based on: - Failure type: transient errors may be
retryable; invalid data usually is not. - Resource impact: repeated
attempts can consume compute. - Downstream dependencies: failed tasks
may block later work. - Business SLA: retry windows must fit processing
deadlines.

### 6.2 Timeout

-   Sets the maximum allowed duration for a task or run, where
    supported.
-   If both timeout and retries are configured, the timeout applies to
    each attempt.
-   Choose thresholds based on expected runtime and SLA.

### 6.3 Repair Run

Use Repair Run to rerun failed or skipped tasks in an existing Job run
after fixing the cause.

-   Successful upstream tasks can be retained rather than rerun.
-   Failed tasks and relevant downstream tasks can be selected for
    repair.
-   Useful for reducing repeated processing and compute cost.
-   Confirm that retained outputs are still valid before repairing.

## 7. Notifications and Alerts

Notifications can be configured at Job level and, where supported, Task
level.

Events include: - Run/task started. - Successful completion. -
Failure. - Duration exceeds a configured threshold. - Streaming backlog
exceeds a configured threshold. - Maintenance start/completion for
supported continuous Jobs.

Destinations include: - Email. - Configured system destinations such as
Slack, Microsoft Teams, and PagerDuty. - HTTP webhooks.

Good practices: - Send Job-level alerts for overall workflow outcomes. -
Add Task-level alerts for critical or independently operated steps. -
Alert on failures and SLA/duration thresholds. - Avoid noisy success
notifications for every low-level task unless needed. - Use webhooks for
custom incident-management integrations.

## 8. Compute

Each task requires compute to execute. Depending on task type and
workspace configuration, options include:

-   **Serverless compute:** Databricks manages provisioning and scaling
    for supported workloads.
-   **Jobs compute:** Dedicated compute for Job workloads; generally
    preferred over interactive compute for production Jobs.
-   **All-purpose compute:** Existing interactive compute, where
    supported.

Choose compute based on workload type, libraries, runtime requirements,
performance, governance, and cost. Some task types manage their own
compute (for example, Lakeflow Pipeline tasks).

## 9. Monitoring and Observability

Use the Jobs UI to inspect: - Active and historical Job runs. -
Task-level run status. - Run duration and timeline. - Task output and
logs. - Error messages and failure causes. - Repair and rerun options.

Common statuses include pending/queued, running, succeeded, failed,
canceled, skipped, and timed out. Exact status labels can vary by
view/API.

### System tables

Databricks provides Job and pipeline activity data in the
`system.lakeflow` schema.

Use system tables to build centralized operational reporting, such as: -
Job success/failure rates. - Execution duration trends. - Frequently
failing tasks. - Long-running Jobs. - Run volume and trigger usage.

System tables require appropriate permissions and have documented
retention and regional scope.

## 10. Permissions, Identity, and Security

Lakeflow Jobs has two distinct security concepts:

1.  **Job permissions:** Determine who can view, run, edit, or manage a
    Job.
2.  **Run as identity:** Determines which user or service principal the
    Job uses to access data and other resources.

Production recommendations: - Prefer a service principal as the Run as
identity for durable production workloads. - Grant least-privilege
access to tables, volumes, external locations, secrets, and other
resources. - Ensure the Run as identity has the permissions required by
every task. - Protect logs and secrets; do not print credentials or
sensitive values.

Permission names and exact capabilities are documented in the workspace
Job access-control settings.

## 11. Git, CI/CD, and Deployment

### Git integration

Supported task types can retrieve source code from a remote Git
repository. Databricks snapshots a specified branch/commit for a Job run
so tasks in that run use a consistent version.

Git-backed tasks include supported Notebook, Python script, SQL file,
and dbt tasks. Check the current documentation for supported task types
and limitations.

### Declarative Automation Bundles

Declarative Automation Bundles (formerly Databricks Asset Bundles) let
you define Jobs and related resources as code and deploy them across
environments.

Typical promotion flow:

``` text
Development
    |
    v
Testing / QA
    |
    v
Production
```

Benefits: - Version-controlled Job definitions. - Repeatable
deployments. - Environment-specific configuration. - CI/CD integration.

## 12. Lakeflow Jobs vs. Lakeflow Spark Declarative Pipelines

  ------------------------------------------------------------------------
  Aspect                  Lakeflow Jobs           Lakeflow Spark
                                                  Declarative Pipelines
  ----------------------- ----------------------- ------------------------
  Primary role            Orchestrate             Define and execute data
                          tasks/workflows         pipelines

  Main building block     Job and Task            Streaming tables and
                                                  materialized views

  Dependency model        Task-level DAG          Dataset/transformation
                                                  dependencies

  Approach                Procedural              Declarative pipeline
                          orchestration           definition

  Typical use             Coordinate ingestion,   Build and maintain ETL
                          transformation,         data flows
                          validation,             
                          notification            
  ------------------------------------------------------------------------

They work together: a Pipeline task in a Lakeflow Job can run a Lakeflow
pipeline as one step in a larger workflow.

## 13. Practical End-to-End Design Example

**Scenario:** Daily customer and order processing.

``` text
Scheduled / File Arrival Trigger
              |
              v
      Ingest Customer Data
              |
      Ingest Order Data
              |
        (parallel tasks)
              |
              v
       Transform Silver
              |
              v
       If/else: quality OK?
          /          \
        Yes           No
         |             |
         v             v
     Build Gold     Send Alert
         |
         v
    Validate Gold
         |
         v
   Success Notification
```

Suggested design: 1. Use a trigger that matches data readiness
(schedule, file arrival, or table update). 2. Pass environment and
processing date as Job parameters. 3. Run independent ingestion tasks in
parallel. 4. Use retries for transient failures and timeouts for runaway
tasks. 5. Publish record counts or validation results as Task Values. 6.
Use If/else for data-driven branching. 7. Use Run if for failure
notifications and cleanup. 8. Use For each when the same transformation
must run for many tables/files. 9. Use Repair Run after fixing a failed
task. 10. Monitor outcomes through Job run history and `system.lakeflow`
tables.

## 14. Common Design Distinctions to Remember

  -----------------------------------------------------------------------
  Do not confuse                      Correct distinction
  ----------------------------------- -----------------------------------
  Job vs Task                         Job orchestrates; Task performs a
                                      unit of work

  Job Run vs Task Run                 One Job execution contains task
                                      executions

  Run if vs If/else                   Run if checks upstream outcomes;
                                      If/else evaluates a condition

  Parameters vs Task Values           Parameters are inputs; Task Values
                                      are runtime outputs

  Dynamic references vs Task Values   Dynamic references are template
                                      expressions used in configuration;
                                      Task Values are explicitly set by
                                      task code

  For each container vs nested task   Container manages iterations;
                                      nested task performs each iteration

  For each concurrency vs maximum     One controls loop iterations; the
  concurrent runs                     other controls simultaneous Job
                                      runs

  File Arrival vs Table Update        File Arrival monitors files in a UC
                                      storage path; Table Update monitors
                                      supported UC tables/views

  UC external table vs external       A UC external Delta table is not
  database table                      the same as a SQL Server foreign
                                      table

  Lakeflow Jobs vs Lakeflow Pipelines Jobs orchestrate; Pipelines define
                                      data transformation flows
  -----------------------------------------------------------------------

## 15. Production Checklist

-   [ ] Use meaningful Job and Task names.
-   [ ] Define explicit dependencies; allow independent tasks to run in
    parallel.
-   [ ] Parameterize environment, dates, and source identifiers.
-   [ ] Use Task Values only for runtime outputs that downstream tasks
    need.
-   [ ] Select triggers based on actual data readiness.
-   [ ] Set maximum concurrent runs and queueing intentionally.
-   [ ] Set For each concurrency according to workspace limits and
    compute capacity.
-   [ ] Configure retries for transient errors, not permanent
    data/configuration errors.
-   [ ] Configure task/run timeouts.
-   [ ] Add failure and SLA notifications without creating alert noise.
-   [ ] Use a service principal and least-privilege permissions for
    production.
-   [ ] Use Jobs compute or serverless where appropriate.
-   [ ] Keep code and Job definitions in version control.
-   [ ] Use deployment automation for dev/test/prod promotion.
-   [ ] Monitor runs, logs, and `system.lakeflow` history.
-   [ ] Test Repair Run, backfills, and failure paths before production
    release.
-   [ ] Ensure ingestion is idempotent and uses checkpoints/watermarks
    where applicable.

## 16. Official Databricks References

-   [Lakeflow Jobs overview](https://docs.databricks.com/aws/en/jobs/)
-   [Configure and edit
    Jobs](https://docs.databricks.com/aws/en/jobs/configure-job)
-   [Configure and edit
    tasks](https://docs.databricks.com/aws/en/jobs/configure-task)
-   [Control flow in
    Jobs](https://docs.databricks.com/aws/en/jobs/control-flow)
-   [Configure task dependencies / Run
    if](https://docs.databricks.com/aws/en/jobs/run-if)
-   [If/else
    tasks](https://docs.databricks.com/aws/en/jobs/tasks/if-else)
-   [For each
    tasks](https://docs.databricks.com/aws/en/jobs/tasks/for-each)
-   [Job parameters](https://docs.databricks.com/aws/en/jobs/parameters)
-   [Dynamic value
    references](https://docs.databricks.com/aws/en/jobs/dynamic-value-references)
-   [Task Values](https://docs.databricks.com/aws/en/jobs/task-values)
-   [Schedules and
    triggers](https://docs.databricks.com/aws/en/jobs/triggers)
-   [File arrival
    triggers](https://docs.databricks.com/aws/en/jobs/file-arrival-triggers)
-   [Table update
    triggers](https://docs.databricks.com/aws/en/jobs/trigger-table-update)
-   [Retries and task
    configuration](https://docs.databricks.com/aws/en/jobs/configure-task)
-   [Notifications](https://docs.databricks.com/aws/en/jobs/notifications)
-   [Repair failed Job
    runs](https://docs.databricks.com/aws/en/jobs/repair-job-failures)
-   [Job permissions and Run as
    identity](https://docs.databricks.com/aws/en/jobs/privileges)
-   [Git integration](https://docs.databricks.com/aws/en/jobs/git)
-   [Jobs system
    tables](https://docs.databricks.com/aws/en/admin/system-tables/jobs)
-   [Run Job
    task](https://docs.databricks.com/aws/en/jobs/tasks/run-job)
-   [Lakeflow Pipeline
    task](https://docs.databricks.com/aws/en/jobs/tasks/pipeline)
-   [Databricks resource
    limits](https://docs.databricks.com/aws/en/resources/limits)

------------------------------------------------------------------------

**Revision summary:** Lakeflow Jobs = Job + Tasks + Dependencies +
Triggers + Parameters + Compute + Failure handling + Monitoring +
Security + Deployment.
