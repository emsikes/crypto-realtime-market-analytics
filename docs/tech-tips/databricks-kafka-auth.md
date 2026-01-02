Shaded Dependencies

Databricks bundles its own version of the Kafka client libraries, but renames the package paths to avoid conflicts with other versions you might install. This is called "shading."
Standard Kafka package:
  org.apache.kafka.common.security.plain.PlainLoginModule

Databricks shaded version:
  kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule
  
When Java tried to find the PlainLoginModule class using the standard path, it couldn't find it—because Databricks renamed it.

Why Databricks Does This
ProblemSolutionYou install Kafka 3.5, Databricks ships Kafka 3.2Shading keeps them separateClass version conflicts crash jobsEach lives in its own namespaceDependency hellAvoided

The Lesson
When connecting to Kafka from Databricks, always use kafkashaded.org.apache.kafka... in the JAAS config string. This is a common gotcha.