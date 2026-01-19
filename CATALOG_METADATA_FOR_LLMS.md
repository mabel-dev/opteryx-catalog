**Catalog Metadata Recommendations for LLM Semantic Layer**

Purpose
- Provide recommended metadata items and annotations to add to datasets, columns, and related catalog entities so LLMs can accurately discover, interpret, and ground answers using the catalog data.

How to read this document
- For each metadata item: **What** the item is, **How to create/store/maintain** it, and **How an LLM will use it**.

1) Dataset-level metadata
- Description
  - What: Concise human-readable overview of dataset purpose, scope, and typical use-cases (2–4 sentences). Link to detailed README when available.
  - Create/Store/Maintain: Store as a catalog field (`description`). Keep in repo or metastore README and surface in the catalog UI. Update via CI when schema or owners change; include last-updated timestamp and version.
  - LLM usage: Provides context for retrieval and prompt grounding; used to choose relevant datasets and to craft natural-language explanations.

- Owner & contacts
  - What: Dataset owner, steward, and on-call contacts; GitHub/Slack/email links.
  - Create/Store/Maintain: Store as structured fields (`owner`, `steward`, `contact`). Sync with org directory or Git metadata; validate via periodic checks.
  - LLM usage: When answers require clarification or approval, LLMs can surface contacts and generate recommended outreach text.

- Sensitivity / Classification / PII tag
  - What: One or more classification labels (e.g., `public`, `internal`, `confidential`, `restricted`, `pii`), plus GDPR/CCPA flags.
  - Create/Store/Maintain: Use controlled vocabulary; store as structured tags. Apply automated PII detectors and manual review. Enforce policy on ingest and propagation to column-level.
  - LLM usage: Filter retrieval and enforce redaction/response constraints; avoid exposing sensitive values; add safety warnings to generated content.

- Freshness & cadence
  - What: `last_updated`, `update_frequency` (daily/hourly/real-time), `data_latency` (how stale data typically is).
  - Create/Store/Maintain: Populate from ingestion pipelines; update automatically in ETL. Include source commit/manifest versions for reproducibility.
  - LLM usage: Prefer fresher sources for time-sensitive answers; indicate confidence based on age of data.

- Row count & high-level statistics
  - What: Approximate `row_count`, record size, table size, partition layout summary.
  - Create/Store/Maintain: Compute during ingestion or via scheduled jobs; update metrics store.
  - LLM usage: Assess reliability and representativeness; help prioritize datasets for retrieval.

- Business domain & canonical terms
  - What: Business domain name (e.g., `billing`, `customer-360`) and canonical dataset tags mapped to an ontology or glossary.
  - Create/Store/Maintain: Maintain a central business glossary; link dataset to canonical terms via IDs.
  - LLM usage: Map user queries to domain-specific datasets; disambiguation and slot-filling during query-generation.

- Lineage & provenance
  - What: Source systems, upstream datasets, transformations, jobs, timestamps, and commit hashes.
  - Create/Store/Maintain: Capture automatically in ETL frameworks (e.g., as part of job metadata). Store as structured lineage graph or DAG references.
  - LLM usage: Provide provenance evidence, justify answers, enable traceability and chain-of-thought grounding.

- Sample rows / schema examples
  - What: Small anonymized sample (10–50 rows) or schema-typed examples demonstrating typical values.
  - Create/Store/Maintain: Generate from dataset with redaction for PII; store as reversible or irreversible sampled snapshot depending on policy.
  - LLM usage: Help the model understand value formats and craft better queries and extraction prompts.

2) Column-level metadata
- Column description
  - What: Natural-language description of what the column represents and business interpretation.
  - Create/Store/Maintain: Add as `description` on column definitions; source from data producers and data stewards. Keep small and precise.
  - LLM usage: Crucial for mapping natural-language attributes to schema fields when generating queries or answering questions.

- Semantic type / ontology mapping
  - What: Semantic tag (e.g., `email`, `currency`, `timestamp`, `country`, `user_id`) and link to canonical ontology term.
  - Create/Store/Maintain: Standardize tags (controlled vocabulary) and map via schema/registry. Use detectors to suggest mappings; require steward approval.
  - LLM usage: Improves normalization, unit-aware reasoning, and safe handling (PII awareness). Helps in entity linking and canonicalization.

- Units & format
  - What: Units (`USD`, `meters`), timezone for timestamps, expected format (ISO date, RFC3339), regex examples.
  - Create/Store/Maintain: Maintain as metadata fields; validate in ETL and during schema checks.
  - LLM usage: Enables correct conversions, comparisons, and localized formatting in answers.

- Value examples / top values
  - What: 5–10 typical or top cardinal values, plus common error patterns.
  - Create/Store/Maintain: Compute periodically; keep derived stats (top-k values, distinct_count, null_fraction).
  - LLM usage: Supports disambiguation, common-case assumptions, and helps avoid hallucinating unexpected values.

- Cardinality & distinct count
  - What: High-cardinality flag, distinct counts, null ratio, and uniqueness (candidate primary key).
  - Create/Store/Maintain: Compute in metrics jobs; update with data refresh.
  - LLM usage: Guide join strategy, entity resolution, and explainability of joins.

- Referential links (foreign keys)
  - What: Declared relationships to other dataset columns (FK -> primary key reference).
  - Create/Store/Maintain: Detect via profiling or declare in schema; validate periodically.
  - LLM usage: Assist query planning, join recommendations, and preserving referential integrity when constructing SQL.

- Derived / computed flag & transform expression
  - What: Whether column is derived; store the transformation SQL/logic or pointer to transformation job.
  - Create/Store/Maintain: Capture in ETL metadata and code repository; version the expression.
  - LLM usage: Explain derivation and enable tracing of how reported values were produced.

3) File/manifest/partition-level metadata
- Storage format & location
  - What: File type (Parquet/CSV/ORC), bucket/path, partitioning scheme and partition keys.
  - Create/Store/Maintain: Store in manifest metadata and file manifest; keep hashes and sizes per file.
  - LLM usage: Answer storage/availability questions; determine efficient access patterns and cost estimates.

- Partition statistics
  - What: Per-partition row counts, min/max values for partition keys, and freshness per partition.
  - Create/Store/Maintain: Aggregated by ingestion jobs; indexed by partition.
  - LLM usage: Helps narrow retrieval to relevant partitions for RAG pipelines and to limit data scanned.

4) Catalog-level artifacts useful to LLMs
- Business glossary & term definitions
  - What: Canonical business definitions, synonyms, and mappings to schema elements.
  - Create/Store/Maintain: Central glossary service or JSON-LD store; tie terms to dataset/column IDs.
  - LLM usage: Disambiguate user language to schema; provide more accurate slot filling and entity resolution.

- Embeddings & vector indexes
  - What: Semantic embeddings for dataset descriptions, column descriptions, sample rows, and business terms.
  - Create/Store/Maintain: Generate with a chosen encoder; store vectors in a vector DB with pointers to canonical IDs and timestamps. Recompute on description or sample updates.
  - LLM usage: Retrieval-augmented generation (RAG): find the most relevant schema pieces, example rows, and docs for prompts.

- Index of FAQ / usage examples / canned queries
  - What: Curated list of example queries, typical SQL snippets, common pitfalls, and recommended joins.
  - Create/Store/Maintain: Curated by data stewards; surfaced via dataset README and catalog UI.
  - LLM usage: Use examples as few-shot context to improve generated queries and recommended actions.

- Data quality rules & test results
  - What: Rules (uniqueness, ranges, not-null) and latest validation outcomes with severity.
  - Create/Store/Maintain: Store test definitions and results in metadata store; CI gating for failing tests.
  - LLM usage: Modify confidence, add caveats to answers, and suggest remediation steps.

5) Formats and storage recommendations
- Use structured, machine-readable metadata (JSON / JSON-LD)
  - Why: LLMs and downstream services can easily parse and ingest structured fields; JSON-LD helps link to ontologies.

- Use a single source-of-truth metastore
  - Why: Consistency for ingestion, tooling and LLM retrieval. Options: existing metastore (Hive/Glue/BigQuery/Firestore), Data Catalog solutions (OpenMetadata, Apache Atlas), or a small dedicated metadata DB linked to the catalog.

- Versioning and immutability
  - Why: Keep historical context available for provenance and reproducibility. Store `schema_version`, `metadata_version`, and stable dataset identifiers.

- Vector store for embeddings
  - Why: Fast semantic retrieval. Keep vector metadata pointing back to canonical IDs; store embedding model name and timestamp.

- Controlled vocabularies and schemas
  - Why: Predictable semantics for LLM prompts. Define enumerations for classification, semantic types, sensitivity, update frequency.

6) Ingestion & maintenance guidance
- Automated extraction
  - Create jobs that generate or update: descriptions (seeded then curated), statistics, sample rows (PII redacted), tests, lineage.

- Review workflow
  - Provide suggested changes automatically (profiling/detectors), but require steward human approval for semantic fields (descriptions, sensitivity, canonical mapping).

- Frequency & triggers
  - Update statistics and embeddings on refresh cycles or schema change; update descriptions and lineage when ETL code or owner changes.

- Audit & access control
  - Maintain audit logs of metadata changes and who changed them. Enforce RBAC for who can change sensitivity or owner.

7) How LLMs will consume and use these items
- Retrieval & grounding
  - LLMs will use dataset and column descriptions, embeddings, and sample rows to select relevant data and ground responses in factual sources.

- Query generation (SQL/filters)
  - Column semantics, units, and sample values let the LLM map natural-language predicates into typed SQL fragments, reducing malformed queries.

- Safety & policy enforcement
  - Use sensitivity tags and PII flags to block or redact results and to insert safety disclaimers in generated output.

- Explainability & provenance
  - Lineage and transform expressions allow the LLM to explain how values were derived and link back to data sources.

- Confidence estimation
  - Use freshness, data quality results, and row counts to set answer confidence and create caveats for the user.

- Disambiguation & entity linking
  - Use business glossary + ontology mappings so the LLM can resolve ambiguous user terms to canonical schema elements.

8) Implementation checklist & examples
- Minimum viable metadata to onboard a dataset
  - `description`, `owner`, `sensitivity`, `last_updated`, column `description`, column `semantic_type`, and `schema`.

- Recommended full set (for production LLM usage)
  - All dataset-level items in section 1, all column-level in section 2, embeddings, glossary links, lineage, DQ rules, and sample rows.

- Example JSON snippet (dataset metadata)
  - {
    "dataset_id": "billing.transactions.v1",
    "description": "Transaction-level events for billing.",
    "owner": "team-billing@example.com",
    "sensitivity": "internal",
    "last_updated": "2026-01-10T12:00:00Z",
    "row_count": 12345678,
    "tags": ["billing","payments"],
    "glossary_terms": ["invoice","chargeback"]
  }

9) Security, privacy, and governance notes
- Redaction and synthetic samples
  - Never store raw PII in sample rows unless explicitly approved; use redaction or synthetic replacements.

- Embedding privacy
  - Beware of embedding models that can memorize; strip raw PII before embedding and record embedding provenance.

- Policy enforcement
  - Enforce access control at retrieval time using sensitivity tags; LLM layer must check authorization before exposing content.

10) Next steps & suggested rollout
- Phase 1 (MVP): Add `description`, `owner`, `sensitivity`, `last_updated`, column `description`, column `semantic_type`, and compute basic stats and sample rows (redacted).
- Phase 2: Add lineage capture, DQ rules, vector embeddings for descriptions and samples, and top-value statistics.
- Phase 3: Full integration with glossary/ontology, automated detectors, and UI surfaces for steward approval.

Contact / Review
- Please review and indicate priority items to implement first. For implementation I can scaffold extractor jobs, a JSON schema for metadata, or a small metastore adapter for this repository.
