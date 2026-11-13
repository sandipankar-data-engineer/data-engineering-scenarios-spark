# Ingesting Enterprise Data Overview

> **Purpose:** Understand how enterprise data is brought into
> Databricks, the role of Lakeflow Connect, the main ingestion
> architectures, and how ingestion is secured, orchestrated, monitored,
> and operated.
>
> This note combines the concepts discussed in our learning session with
> additional details from Databricks documentation. Connector
> availability and behavior can vary by cloud, region, source, and
> release status. Verify the current connector documentation before
> implementation.

------------------------------------------------------------------------

## 1. What is enterprise data ingestion?

Enterprise data ingestion is the process of collecting data from
business applications, operational databases, file repositories, and
event platforms and bringing it into a central analytics platform such
as Databricks.

**Typical enterprise sources**

-   SaaS applications: Salesforce, Workday, HubSpot, Jira, ServiceNow.
-   Relational databases: PostgreSQL, SQL Server, MySQL, Oracle.
-   Enterprise file services: SharePoint, Google Drive, OneDrive.
-   Cloud object storage: Amazon S3, Azure Data Lake Storage (ADLS),
    Google Cloud Storage (GCS).
-   Streaming platforms: Kafka and other supported message buses.

**Why ingest enterprise data?**

-   Build a unified view of business data across systems.
-   Enable analytics, reporting, machine learning, and AI workloads.
-   Preserve source data for downstream transformation and historical
    analysis.
-   Reduce repeated direct queries against operational systems.
-   Make data available to downstream teams through governed lakehouse
    tables.

------------------------------------------------------------------------

## 2. What is Lakeflow Connect?

**Lakeflow Connect** is Databricks' data ingestion offering for bringing
data from external enterprise systems into the Databricks Lakehouse.

It provides connectors and ingestion frameworks for different source
types. Fully managed connectors automate much of the source-specific
work, such as authentication, incremental ingestion, retries, and schema
handling.

**Key capabilities**

-   Managed ingestion from supported enterprise applications and
    databases.
-   Incremental reads and writes where supported by the source and
    connector.
-   Source authentication managed through Unity Catalog connections.
-   Integration with Lakeflow pipelines and Lakeflow Jobs.
-   Destination tables governed through Unity Catalog.
-   Monitoring through pipeline event logs, metrics, and logs.
-   Different levels of customization, from fully managed connectors to
    custom ingestion code.

**Important distinction:** Lakeflow Connect is primarily about bringing
data into Databricks. It is not the same as Partner Connect, which helps
connect a Databricks workspace to selected third-party partner products.

------------------------------------------------------------------------

## 3. Lakeflow Connect connector categories

  -----------------------------------------------------------------------
  Connector category      Source examples         Typical purpose
  ----------------------- ----------------------- -----------------------
  SaaS connectors         Salesforce, Workday,    Ingest application and
                          HubSpot, Jira           business-process data

  Database CDC connectors PostgreSQL, SQL Server, Capture database
                          MySQL, Oracle           snapshots and ongoing
                                                  changes

  Query-based connectors  Supported relational    Query source tables on
                          databases and federated a schedule using a
                          sources                 cursor column

  File connectors         SharePoint, Google      Ingest files from
                          Drive, OneDrive         enterprise file
                                                  services

  Streaming connectors    Supported message       Continuously ingest
                          buses, such as RabbitMQ events/messages

  Community connectors    Community-maintained    Extend coverage where a
                          sources                 managed connector is
                                                  unavailable

  Custom connectors       Organization-specific   Build and operate a
                          or unsupported sources  connector for a
                                                  specialized source
  -----------------------------------------------------------------------

**Note:** Not every connector category or source has identical
capabilities. Supported sources, ingestion modes, release stages, and
interfaces vary. Check connector-specific documentation before designing
around a feature.

------------------------------------------------------------------------

## 4. Managed connectors

Managed connectors are the most automated Lakeflow Connect option for
supported sources.

**What do they provide?**

-   Source-specific authentication handling.
-   Incremental ingestion where supported.
-   Automated retries and recovery behavior.
-   Schema evolution handling, subject to connector behavior and
    configuration.
-   Integration with Unity Catalog and Lakeflow pipelines.
-   Reduced need to write and maintain custom extraction code.

**How should we choose an ingestion approach?**

Databricks recommends starting with the most managed option that
satisfies the requirement. If a managed connector does not support the
source, required feature, or customization, move to a more customizable
approach.

1.  Fully managed connector, if the source and required behavior are
    supported.
2.  Standard SQL or Spark APIs / Lakeflow pipelines, if more control is
    required.
3.  Community or custom connector / DIY ingestion, if the source is not
    covered by available managed options.

------------------------------------------------------------------------

## 5. SaaS ingestion architecture

SaaS sources are typically external applications accessed through APIs
or supported application endpoints.

### How does it work?

**External SaaS Source**

Salesforce / Workday / Public API / Other supported SaaS application

Provides application data through its supported API or endpoint.

↓

**Unity Catalog Connection**

Stores source authentication information and provides a governed way for
the pipeline to access credentials.

↓

**Lakeflow Serverless Declarative Pipeline**

Uses the connection to authenticate with the source, retrieves data, and
performs the configured ingestion processing.

↓

**Streaming Delta Tables**

Stores the ingested data in Databricks as streaming tables (Delta tables
with support for incremental processing).

### Main components

**External SaaS Source**

-   The system that owns the business data.
-   Examples include CRM, HR, marketing, ticketing, and work-management
    applications.
-   API access, permissions, rate limits, and source-side retention can
    affect ingestion.

**Unity Catalog Connection**

-   A Unity Catalog securable object used to store source authentication
    details.
-   Avoids hardcoding credentials directly into pipeline definitions.
-   Access is controlled through Unity Catalog privileges.

**Lakeflow Serverless Declarative Pipeline**

-   Runs the managed ingestion pipeline on serverless compute by default
    for managed SaaS connectors.
-   Uses the configured connection to authenticate.
-   Reads selected source objects and writes data to destination tables.
-   Can be run according to a configured schedule.

**Streaming Delta Tables**

-   Destination tables for ingested records.
-   Support incremental data processing.
-   Can be used by downstream transformation and analytics pipelines.

### Example: Salesforce

1.  Open the Databricks data-ingestion experience.
2.  Select the Salesforce connector.
3.  Create or select a Unity Catalog connection.
4.  Authenticate to Salesforce; configure sandbox access if the source
    environment requires it.
5.  Select the Salesforce objects to ingest.
6.  Configure the destination catalog/schema and ingestion settings.
7.  Configure the schedule.
8.  Run the pipeline and monitor its progress and destination tables.

A schedule such as every six hours is only an example; select a
frequency that meets the freshness requirement and respects source API
limits.

------------------------------------------------------------------------

## 6. Database ingestion architecture: CDC-based managed connector

Database CDC connectors are designed to capture an initial snapshot and
then track ongoing changes from supported relational databases.

### How does it work?

**External Database**

PostgreSQL / SQL Server / MySQL / Oracle / Other supported database

Hosts the operational tables and change information.

↓

**Ingestion Gateway**

Classic compute Declarative Pipelines job.

-   Connects to the source database.
-   Extracts metadata, initial snapshots, and change logs.
-   Continuously captures changes so source logs are read before they
    can be truncated.
-   Stages extracted data for the ingestion pipeline.

↓

**Unity Catalog Volume**

Intermediate staging layer.

-   Stores extracted snapshot and change data temporarily.
-   Decouples continuous source capture from destination-table
    processing.
-   Supports recovery and allows the downstream ingestion pipeline to
    run on its own schedule.
-   Staging data is automatically purged after the retention period
    documented by Databricks (currently 30 days in the referenced
    documentation).

↓

**Serverless Declarative Pipeline**

-   Reads staged data.
-   Applies snapshot and incremental changes.
-   Writes the results to destination tables.

↓

**Streaming Delta Tables**

Final ingestion destination.

### Main components

**External Database**

-   Source relational database, such as PostgreSQL, SQL Server, MySQL,
    or Oracle.
-   The source must be configured with the required permissions and
    CDC/change-capture prerequisites for the selected connector.

**Unity Catalog Connection**

-   Stores the database authentication details.
-   The gateway uses the connection to authenticate to the source.

**Ingestion Gateway**

-   A dedicated ingestion pipeline.
-   Runs on classic compute.
-   Connects to the source database over the network.
-   Captures initial snapshots, metadata, and ongoing change events.
-   Runs continuously for CDC-based database ingestion.

**Unity Catalog Volume (staging storage)**

-   Temporary intermediate storage between the gateway and ingestion
    pipeline.
-   Holds extracted source data before it is applied to destination
    tables.
-   Helps decouple the gateway's continuous capture from the downstream
    pipeline schedule.
-   Provides a buffer that helps with recovery when downstream
    processing is delayed or fails.

**Serverless Declarative Pipeline**

-   Processes staged data.
-   Applies changes to destination streaming tables.
-   Can run on a schedule independently of the continuously running
    gateway.

**Streaming Delta Tables**

-   Final target tables.
-   Store the ingested data in Delta format.
-   Feed downstream transformations, analytics, and reporting.

### Why is the ingestion gateway important?

**1. Network connectivity**

-   The source database may be inside a private network or behind a
    firewall.
-   The gateway runs in the Databricks workspace network environment and
    must be able to reach the database.
-   Connectivity can be established through supported network paths such
    as VPN, peering, or private connectivity options, depending on the
    cloud architecture.

**2. Continuous change capture**

-   The gateway continuously captures database changes.
-   This is important because source database logs can be truncated or
    recycled.
-   The gateway should be sized according to the workload and initial
    snapshot requirements.

**3. Separation of responsibilities**

-   The gateway focuses on extracting and staging source changes.
-   The downstream serverless pipeline focuses on applying those changes
    to destination tables.
-   The two components can operate on different schedules.

**4. Operational isolation**

-   The gateway and staging layer reduce the need for the downstream
    processing pipeline to repeatedly connect to the source database.
-   This can help reduce direct source access and separate source
    extraction from target processing.

------------------------------------------------------------------------

## 7. Database ingestion architecture: query-based connector

Not all database ingestion requires CDC.

A query-based connector reads source tables directly on a schedule. It
uses a cursor column to identify rows that are new or have changed since
the previous successful run.

### How does it work?

**External Database**

Supported relational database or federated source.

Contains source tables and a suitable cursor column, such as
`updated_at`.

↓

**Unity Catalog Connection / Foreign Catalog**

Provides governed connectivity and authentication to the source.

↓

**Scheduled Query-based Ingestion Pipeline**

-   Queries the source directly.
-   Reads rows whose cursor value is greater than the previously stored
    high-water mark.
-   Stores the new high-water mark after a successful run.

↓

**Streaming Delta Tables**

Receives the ingested records.

### Query-based ingestion vs CDC-based ingestion

  -----------------------------------------------------------------------
  Aspect                  CDC-based database      Query-based connector
                          connector               
  ----------------------- ----------------------- -----------------------
  Change detection        Database change logs /  Cursor column
                          CDC mechanisms          

  Gateway                 Required                Not required

  Staging volume          Used                    Not required

  Source access           Captures changes        Queries source tables
                          through the gateway     directly

  Execution               Gateway continuously    Pipeline runs on a
                          captures changes;       schedule
                          ingestion pipeline      
                          processes staged data   

  Intermediate row states Can capture changes     Generally captures the
                          represented in the      latest qualifying row
                          source change log       state between runs

  Source load             Reads change logs after Runs queries against
                          snapshot;               source tables
                          source-specific         
                          behavior applies        

  Suitable when           Low-latency change      CDC is unavailable or
                          capture is needed and   not required and a
                          CDC prerequisites are   suitable cursor column
                          available               exists
  -----------------------------------------------------------------------

### Cursor column considerations

-   Prefer a timestamp column such as `updated_at` or `last_modified`.
-   The cursor must increase monotonically.
-   The source must update the cursor whenever a row changes.
-   A simple increasing ID is appropriate mainly for append-only data;
    it is not sufficient for detecting updates to existing rows.
-   Rows with a NULL cursor value are not ingested.
-   The connector stores a high-water mark and uses it to determine what
    to read on the next run.
-   Query-based ingestion is scheduled, not continuous CDC.

### History tracking modes

Depending on the connector and configuration, query-based ingestion
supports:

-   `SCD_TYPE_1`: Keep the latest version of a row; previous values are
    overwritten.
-   `SCD_TYPE_2`: Preserve row history by adding new versions with
    validity metadata.
-   `APPEND_ONLY`: Append ingested rows without merging or overwriting
    existing rows.

------------------------------------------------------------------------

## 8. SaaS vs database ingestion

  -----------------------------------------------------------------------
  Aspect                  SaaS ingestion          Database CDC ingestion
  ----------------------- ----------------------- -----------------------
  Source                  Salesforce, Workday,    PostgreSQL, SQL Server,
                          other supported SaaS    MySQL, Oracle
                          apps                    

  Authentication          Unity Catalog           Unity Catalog
                          connection              connection

  Main ingestion          Serverless Declarative  Ingestion Gateway +
  component               Pipeline                Serverless Declarative
                                                  Pipeline

  Gateway                 No database CDC gateway Yes

  Staging volume          No separate gateway     Unity Catalog Volume
                          staging volume in the   
                          standard SaaS           
                          architecture            

  Source extraction       API/application         Snapshot and change-log
                          connector               capture

  Processing              Serverless pipeline     Serverless pipeline
                                                  processes staged data

  Destination             Streaming Delta Tables  Streaming Delta Tables
  -----------------------------------------------------------------------

**Remember**

-   SaaS: Source → Unity Catalog Connection → Serverless Pipeline →
    Streaming Delta Tables.
-   Database CDC: Source → Connection → Ingestion Gateway → Unity
    Catalog Volume → Serverless Pipeline → Streaming Delta Tables.
-   Query-based database ingestion: Source → Connection / Foreign
    Catalog → Scheduled Query-based Pipeline → Streaming Delta Tables.

------------------------------------------------------------------------

## 9. File ingestion and enterprise file sources

Enterprise data is not limited to applications and databases. It can
also reside in file repositories and cloud object storage.

**Managed file connectors**

-   Supported enterprise file services include Google Drive, OneDrive,
    and SharePoint, depending on cloud and connector availability.
-   Managed connectors can handle source authentication, incremental
    reads, and retries.
-   Data is ingested through a pipeline into destination tables.

**Standard file ingestion options**

-   Cloud object storage such as S3, ADLS, and GCS.
-   Auto Loader for scalable incremental file discovery and ingestion.
-   `COPY INTO` for file-based loading.
-   Spark and SQL reader APIs for custom processing.
-   File upload to Unity Catalog Volumes for suitable manual or
    smaller-scale workflows.

**Important distinction:** Managed file connectors and standard file
ingestion APIs are related ingestion options, but they are not the same
implementation. Select the approach based on source support, file
volume, latency, schema behavior, and required control.

------------------------------------------------------------------------

## 10. Streaming source ingestion

Streaming connectors are intended for event sources that continuously
produce messages.

**Typical sources**

-   Supported message buses and event-streaming systems.
-   Examples in Databricks ingestion documentation include RabbitMQ and
    other streaming technologies.

### How does it work?

**Streaming Source**

Message bus / Event streaming platform

Produces events continuously.

↓

**Unity Catalog Connection**

Stores the source endpoint and authentication details.

↓

**Streaming Ingestion Pipeline**

Continuously reads messages from the source using the configured
connector.

↓

**Streaming Delta Tables**

Stores the ingested event data for downstream processing.

For sources or requirements that are not covered by managed streaming
connectors, Structured Streaming and supported SQL/Spark APIs provide
more customization.

------------------------------------------------------------------------

## 11. Unity Catalog's role in enterprise ingestion

Unity Catalog is the governance layer used by Lakeflow Connect.

**Connections**

-   Store authentication details for external sources.
-   Provide a governed object through which pipelines connect to source
    systems.
-   Privileges such as `USE CONNECTION` control which users can use a
    connection.

**Volumes**

-   Provide governed file storage.
-   Database CDC ingestion uses a Unity Catalog Volume as staging
    storage between the gateway and the ingestion pipeline.

**Catalogs and schemas**

-   Organize destination tables.
-   Apply access controls to the ingested data.
-   Provide a consistent namespace for downstream users and pipelines.

**Destination tables**

-   Managed ingestion pipelines write to destination streaming tables.
-   Unity Catalog governs access to these tables and supports lineage
    and discovery.

**Security reminders**

-   Avoid embedding passwords, tokens, or secrets in notebooks and
    pipeline code.
-   Grant only the privileges needed to create and run the ingestion
    pipeline.
-   Validate source-side permissions independently from Databricks
    permissions.
-   Review network access, secret rotation, and service-account
    ownership.

------------------------------------------------------------------------

## 12. Incremental ingestion: initial load and subsequent runs

Incremental ingestion avoids repeatedly copying an entire source dataset
when only a subset has changed.

### Typical lifecycle

**First successful ingestion**

-   The connector reads the selected source data.
-   For CDC-based database ingestion, this includes an initial snapshot.
-   The connector establishes or records the relevant change-tracking
    position.

↓

**Change tracking**

-   The connector tracks source changes using the mechanism supported by
    the source.
-   Examples include CDC logs, change tracking, or a cursor column.

↓

**Subsequent ingestion**

-   The connector reads changes since the previous successful position,
    where incremental ingestion is supported.
-   The destination is updated with the new or changed records.

### Benefits

-   Reduces repeated full-table reads.
-   Reduces unnecessary data transfer.
-   Can reduce source-system load compared with repeated full extracts.
-   Improves freshness and pipeline efficiency.
-   Supports scalable ingestion for large datasets.

### Important considerations

-   Not every source or table supports incremental ingestion.
-   The exact mechanism differs by connector.
-   Some connectors use CDC; others use a cursor column or
    source-specific incremental logic.
-   Initial snapshots can be large and may need careful sizing and
    scheduling.
-   Deletes, late-arriving records, and schema changes must be validated
    for the chosen connector.

------------------------------------------------------------------------

## 13. Orchestration and scheduling

Lakeflow Connect ingestion pipelines integrate with Lakeflow Jobs.

**SaaS and scheduled ingestion**

-   Configure one or more schedules for the ingestion pipeline.
-   Databricks can create a job for each configured schedule.
-   The ingestion pipeline runs as a task within the job.
-   Additional tasks can be added when the workflow requires them.

**Database CDC ingestion**

-   The ingestion gateway runs as a continuous task in its own job.
-   The downstream ingestion pipeline can run on a separate schedule.
-   This allows source change capture to continue even when the
    destination-processing pipeline is not currently running.

**Design questions**

-   How fresh must the data be?
-   How frequently can the source API or database be queried?
-   Is continuous capture required, or is scheduled ingestion
    sufficient?
-   What is the expected initial-load duration?
-   What happens if a run fails or is delayed?
-   Are downstream tasks dependent on the ingestion finishing?

------------------------------------------------------------------------

## 14. Monitoring and troubleshooting

Lakeflow Connect provides monitoring capabilities that help identify
pipeline health and ingestion issues.

**What should be monitored?**

-   Pipeline run status and duration.
-   Source authentication and connectivity failures.
-   Number of records and bytes ingested.
-   Snapshot progress.
-   CDC discovery and processing latency, where available.
-   Schema evolution and data-quality issues.
-   Event logs and pipeline logs.
-   Compute and ingestion cost.

**Database gateway monitoring**

Gateway event logs can provide per-table progress information for
snapshot and CDC phases.

Useful metrics include:

-   Rows upserted.
-   Rows deleted, where applicable.
-   Output bytes.
-   Snapshot progress percentage.
-   Estimated snapshot completion time, where supported.
-   CDC discovery latency and batch-processing time, where supported.

**Operational checks**

1.  Confirm that the source system is reachable.
2.  Validate source credentials and permissions.
3.  Check whether the source connector is configured with the required
    prerequisites.
4.  Review pipeline and gateway event logs.
5.  Verify that the expected source objects/tables are selected.
6.  Check whether the pipeline is progressing or repeatedly retrying.
7.  Validate destination table counts and recent timestamps.
8.  Investigate schema changes, API limits, and source-side throttling.
9.  Check compute sizing and cost.

**Recovery behavior**

-   Managed connectors can retry transient failures with backoff.
-   Some failures require intervention, such as expired credentials or
    changed source permissions.
-   Where supported, connectors retain their last successful incremental
    position so they can resume without intentionally skipping changes.
-   A full refresh can be used when destination data is inconsistent or
    needs to be reingested, but it should be planned because it can
    create significant source and compute load.

------------------------------------------------------------------------

## 15. Schema evolution and data quality

Enterprise source schemas can change over time.

Examples:

-   A new column is added to a Salesforce object.
-   A database column changes type.
-   A source API introduces a new field.
-   A file contains additional columns or unexpected values.

**What to consider**

-   Understand how the specific connector handles new columns and type
    changes.
-   Review schema evolution settings and supported behavior.
-   Validate destination schemas after source changes.
-   Monitor pipeline events for schema-related failures.
-   Define how incompatible changes should be handled.
-   Add downstream data-quality checks for required fields, keys, nulls,
    and business rules.

Do not assume that all connectors handle every schema change
identically. Connector-specific behavior and configuration matter.

------------------------------------------------------------------------

## 16. Lakeflow Connect vs Partner Connect

These two Databricks capabilities solve different problems.

### Lakeflow Connect

**Purpose:** Bring data from external sources into Databricks.

Examples:

-   Ingest Salesforce data.
-   Ingest Workday data.
-   Ingest PostgreSQL or SQL Server data.
-   Ingest supported enterprise files or streaming events.

### Partner Connect

**Purpose:** Connect a Databricks workspace to selected third-party
technology partner solutions through the Databricks UI.

### How does it work?

**Databricks Workspace**

Contains the lakehouse data that you want to use.

↓

**Partner Connect**

Browse selected partner integrations from the Databricks UI.

↓

**Partner Solution**

Create or connect the partner account and configure the integration.

↓

**Evaluate and Adopt**

Use the partner solution with Databricks data and decide whether it
meets the business requirement.

Partner Connect can provision required Databricks resources for
supported integrations, such as a SQL warehouse, service principal, or
personal access token. The exact resources and connection workflow
depend on the partner.

### Comparison

  -----------------------------------------------------------------------
  Aspect                  Lakeflow Connect        Partner Connect
  ----------------------- ----------------------- -----------------------
  Main purpose            Ingest external data    Connect Databricks to
                          into Databricks         partner products

  Direction of focus      Source systems →        Databricks workspace ↔
                          Databricks              partner solution

  Primary use             Data ingestion          Third-party tool
                                                  integration and
                                                  evaluation

  Example                 Salesforce → Delta      Connect a partner BI or
                          tables                  data tool to Databricks

  Main outcome            Data becomes available  Partner product can
                          in the Lakehouse        work with Databricks
                                                  resources/data
  -----------------------------------------------------------------------

**Remember:** Lakeflow Connect is an ingestion capability. Partner
Connect is an integration path to selected partner solutions.

------------------------------------------------------------------------

## 17. Other enterprise ingestion approaches

Lakeflow Connect is not the only way to ingest data into Databricks.

  -----------------------------------------------------------------------
  Approach                            When it is useful
  ----------------------------------- -----------------------------------
  Lakeflow Connect managed connectors A supported enterprise source and
                                      managed ingestion behavior are
                                      available

  Lakeflow pipelines with SQL/Spark   More control is needed over
                                      ingestion logic and transformations

  Auto Loader                         Incrementally ingest files arriving
                                      in cloud object storage

  `COPY INTO`                         Load files from supported storage
                                      locations using SQL

  Structured Streaming                Build customized streaming
                                      ingestion from supported sources

  Lakehouse Federation                Query supported external systems
                                      without first copying all data into
                                      Databricks

  Community/custom connectors         A managed connector is unavailable
                                      or source-specific logic is
                                      required

  Third-party ingestion tools         An organization already uses an
                                      ETL/ELT or replication product that
                                      integrates with Databricks
  -----------------------------------------------------------------------

**Ingestion vs federation**

-   **Ingestion:** Copies data into Databricks storage and destination
    tables.
-   **Federation:** Allows querying supported external data sources
    without necessarily moving the data into Databricks first.

These approaches are not interchangeable. Choose based on latency,
source capabilities, governance, cost, performance, and whether data
must be physically stored in the Lakehouse.

------------------------------------------------------------------------

## 18. Practical design checklist

Before implementing an enterprise ingestion pipeline, answer the
following questions.

### Source assessment

-   What is the source system and source type?
-   Is there a managed connector?
-   Which tables, objects, files, or event streams are required?
-   What is the source data volume and expected growth?
-   Does the source support CDC, change tracking, or a reliable cursor
    column?
-   Are there API quotas, rate limits, or source maintenance windows?

### Ingestion design

-   Is batch, scheduled incremental, or continuous ingestion required?
-   Is an initial full snapshot required?
-   How will inserts, updates, and deletes be captured?
-   Is a gateway required?
-   Is staging storage required?
-   What is the expected data freshness SLA?
-   How will backfills and full refreshes be handled?

### Security and networking

-   Where will credentials be stored?
-   Which Unity Catalog privileges are required?
-   Can the pipeline or gateway reach the source over the network?
-   Are private connectivity, firewall rules, or IP allowlists required?
-   How will credentials be rotated?
-   Who owns the source service account?

### Reliability and operations

-   What is the retry and recovery strategy?
-   How will failures and stalled ingestion be detected?
-   How will data completeness be validated?
-   How will schema changes be handled?
-   What are the compute and storage cost expectations?
-   Who owns support and operational alerts?

### Destination and downstream use

-   Which catalog and schema will contain the destination tables?
-   What naming conventions will be used?
-   Is history preservation required?
-   Which downstream pipelines depend on the ingested data?
-   What access controls apply to consumers?

------------------------------------------------------------------------

## 19. Quick revision sheet

  -----------------------------------------------------------------------
  Topic                               Remember
  ----------------------------------- -----------------------------------
  Lakeflow Connect                    Databricks capability for managed
                                      and customizable data ingestion

  Managed connector                   Automates source-specific ingestion
                                      work for supported sources

  SaaS architecture                   Connection → Serverless ingestion
                                      pipeline → Streaming Delta Tables

  Database CDC architecture           Connection → Ingestion Gateway → UC
                                      Volume → Serverless ingestion
                                      pipeline → Streaming Delta Tables

  Query-based database ingestion      Direct scheduled queries using a
                                      cursor column; no CDC gateway or
                                      staging volume

  Ingestion Gateway                   Classic compute pipeline that
                                      captures database snapshots and
                                      change logs

  Unity Catalog Volume                Temporary staging storage for
                                      database CDC ingestion

  Streaming Delta Table               Destination table supporting
                                      incremental processing

  Incremental ingestion               First load plus subsequent
                                      changed-data ingestion, when
                                      supported

  Unity Catalog Connection            Governed storage of source
                                      authentication details

  Lakeflow Jobs                       Orchestrates ingestion pipeline
                                      runs and schedules

  Monitoring                          Pipeline health, event logs,
                                      progress, latency, data quality,
                                      and cost

  Partner Connect                     Connects the Databricks workspace
                                      to selected partner solutions

  Lakehouse Federation                Query external data without
                                      necessarily ingesting it into
                                      Databricks
  -----------------------------------------------------------------------

### Architecture flows to memorize

**SaaS ingestion**

External SaaS Source\
↓\
Unity Catalog Connection\
↓\
Lakeflow Serverless Declarative Pipeline\
↓\
Streaming Delta Tables

**Database CDC ingestion**

External Database\
↓\
Unity Catalog Connection\
↓\
Ingestion Gateway (Classic Compute)\
↓\
Unity Catalog Volume\
↓\
Serverless Declarative Pipeline\
↓\
Streaming Delta Tables

**Query-based database ingestion**

External Database\
↓\
Unity Catalog Connection / Foreign Catalog\
↓\
Scheduled Query-based Ingestion Pipeline\
↓\
Streaming Delta Tables

**Managed file ingestion**

Enterprise File Service\
↓\
Unity Catalog Connection\
↓\
Managed File Ingestion Pipeline\
↓\
Streaming Delta Tables

------------------------------------------------------------------------

## 20. Official Databricks documentation

The following official documentation was used to expand and verify the
concepts in this note. Connector availability and details can change, so
use these pages as the source of truth when implementing a pipeline.

1.  [What is Lakeflow
    Connect?](https://docs.databricks.com/aws/en/ingestion/overview)
2.  [Lakeflow Connect connector
    concepts](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/)
3.  [Managed SaaS
    connectors](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/saas-overview)
4.  [Managed database
    connectors](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/cdc-overview)
5.  [Query-based
    connectors](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/query-based-overview)
6.  [File
    connectors](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/file-connectors-overview)
7.  [Common patterns for managed ingestion
    pipelines](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/common-patterns)
8.  [Monitor ingestion gateway progress with event
    logs](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/gateway-event-logs)
9.  [What is Databricks Partner
    Connect?](https://docs.databricks.com/aws/en/partner-connect/)
10. [Connect to managed ingestion
    sources](https://docs.databricks.com/gcp/en/connect/managed-ingestion)
11. [Ingest with SQL and Spark
    APIs](https://docs.databricks.com/aws/en/ingestion)

------------------------------------------------------------------------

**End of notes**
