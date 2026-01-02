┌─────────────────────────────────────────────────────────────┐
│                    Databricks CLI (host os)                 │
│                                                             │
│  databricks secrets put-secret crypto-pipeline \            │
│    CONFLUENT_API_KEY --string-value "abc123"                │
│                          │                                  │
└──────────────────────────┼───────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              Databricks Control Plane                       │
│                                                             │
│   Secret Scope: "crypto-pipeline"                           │
│   ┌─────────────────────────────────────────────┐           │
│   │  CONFLUENT_BOOTSTRAP_SERVERS → [encrypted]  │           │
│   │  CONFLUENT_API_KEY           → [encrypted]  │           │
│   │  CONFLUENT_API_SECRET        → [encrypted]  │           │
│   └─────────────────────────────────────────────┘           │
│                                                             │
└──────────────────────────┬───────────────────────────────────┘
                           │
                           ▼  (at notebook runtime)
┌─────────────────────────────────────────────────────────────┐
│                  Databricks Notebook                        │
│                                                             │
│  dbutils.secrets.get(scope="crypto-pipeline",               │
│                      key="CONFLUENT_API_KEY")               │
│                          │                                  │
│                          ▼                                  │
│                  Returns: "abc123"                          │
│                  (decrypted, in memory only)                │
└─────────────────────────────────────────────────────────────┘