# Production Deployment

This guide covers deploying Spark Pipeline Framework applications to production cluster environments.

## Deployment Modes

Spark Pipeline Framework supports all standard Spark deployment modes:

| Mode | Use Case | Resource Manager |
|------|----------|------------------|
| Local | Development, testing | None |
| YARN | Hadoop clusters | YARN ResourceManager |
| Kubernetes | Cloud-native deployments | K8s API Server |
| Standalone | Dedicated Spark clusters | Spark Master |

## Packaging Your Application

### Fat JAR with sbt-assembly

For production deployments, build a fat JAR containing your pipeline components:

```scala
// build.sbt
lazy val myPipeline = (project in file("."))
  .settings(
    assembly / mainClass := Some("io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner"),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case "reference.conf"         => MergeStrategy.concat
      case _                        => MergeStrategy.first
    }
  )
```

Build the assembly:
```bash
sbt assembly
```

### Dependencies

Your fat JAR should include:
- Your pipeline components
- `spark-pipeline-core`
- `spark-pipeline-runtime`
- `spark-pipeline-runner`

Spark libraries are `provided` scope and supplied by the cluster.

## YARN Deployment

### Client Mode

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --class io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner \
  --driver-memory 2g \
  --executor-memory 4g \
  --executor-cores 2 \
  --num-executors 10 \
  --conf spark.yarn.maxAppAttempts=1 \
  /path/to/my-pipeline-assembly.jar \
  -Dconfig.file=/path/to/pipeline.conf
```

### Cluster Mode

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner \
  --driver-memory 2g \
  --executor-memory 4g \
  --executor-cores 2 \
  --num-executors 10 \
  --files /path/to/pipeline.conf \
  --conf spark.driver.extraJavaOptions=-Dconfig.file=pipeline.conf \
  /path/to/my-pipeline-assembly.jar
```

**Note:** In cluster mode, config files must be distributed via `--files` and referenced by filename only.

### YARN Configuration Tips

```json
# pipeline.conf
spark {
  app-name = "Production Pipeline"
  config {
    "spark.yarn.queue" = "production"
    "spark.yarn.tags" = "pipeline,batch"
    "spark.dynamicAllocation.enabled" = "true"
    "spark.dynamicAllocation.minExecutors" = "5"
    "spark.dynamicAllocation.maxExecutors" = "50"
  }
}
```

## Kubernetes Deployment

### Prerequisites

- Kubernetes cluster with Spark Operator or spark-submit support
- Docker image with Spark and your pipeline JAR
- ServiceAccount with appropriate RBAC permissions

### Dockerfile Example

```dockerfile
FROM apache/spark:3.5.0

USER root

# Copy pipeline JAR
COPY target/scala-2.13/my-pipeline-assembly.jar /opt/spark/jars/

# Copy config files
COPY conf/ /opt/spark/conf/

USER spark
```

### spark-submit to Kubernetes

```bash
spark-submit \
  --master k8s://https://<k8s-api-server>:6443 \
  --deploy-mode cluster \
  --name my-pipeline \
  --class io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner \
  --conf spark.kubernetes.container.image=my-registry/my-pipeline:latest \
  --conf spark.kubernetes.namespace=spark-jobs \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.driver.request.cores=1 \
  --conf spark.kubernetes.executor.request.cores=2 \
  --conf spark.executor.instances=5 \
  --conf spark.driver.extraJavaOptions=-Dconfig.file=/opt/spark/conf/pipeline.conf \
  local:///opt/spark/jars/my-pipeline-assembly.jar
```

### Spark Operator (SparkApplication CRD)

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: my-pipeline
  namespace: spark-jobs
spec:
  type: Scala
  mode: cluster
  image: my-registry/my-pipeline:latest
  imagePullPolicy: Always
  mainClass: io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner
  mainApplicationFile: local:///opt/spark/jars/my-pipeline-assembly.jar
  sparkVersion: "3.5.0"
  driver:
    cores: 1
    memory: "2g"
    serviceAccount: spark
    javaOptions: "-Dconfig.file=/opt/spark/conf/pipeline.conf"
  executor:
    cores: 2
    instances: 5
    memory: "4g"
  restartPolicy:
    type: OnFailure
    onFailureRetries: 3
    onFailureRetryInterval: 10
```

## Amazon EMR

### EMR on EC2

```bash
aws emr add-steps \
  --cluster-id j-XXXXXXXXXXXXX \
  --steps Type=Spark,Name="My Pipeline",ActionOnFailure=CONTINUE,Args=[
    --class,io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner,
    --deploy-mode,cluster,
    --driver-memory,2g,
    --executor-memory,4g,
    --conf,spark.driver.extraJavaOptions=-Dconfig.file=s3://my-bucket/config/pipeline.conf,
    s3://my-bucket/jars/my-pipeline-assembly.jar
  ]
```

### EMR on EKS

```bash
aws emr-containers start-job-run \
  --virtual-cluster-id <virtual-cluster-id> \
  --name "my-pipeline" \
  --execution-role-arn <execution-role-arn> \
  --release-label emr-6.15.0-latest \
  --job-driver '{
    "sparkSubmitJobDriver": {
      "entryPoint": "s3://my-bucket/jars/my-pipeline-assembly.jar",
      "entryPointArguments": [],
      "sparkSubmitParameters": "--class io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner --conf spark.driver.extraJavaOptions=-Dconfig.file=s3://my-bucket/config/pipeline.conf"
    }
  }'
```

### EMR Serverless

```bash
aws emr-serverless start-job-run \
  --application-id <application-id> \
  --execution-role-arn <execution-role-arn> \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://my-bucket/jars/my-pipeline-assembly.jar",
      "entryPointArguments": [],
      "sparkSubmitParameters": "--class io.github.dwsmith1983.spark.pipeline.runner.SimplePipelineRunner --conf spark.driver.extraJavaOptions=-Dconfig.file=s3://my-bucket/config/pipeline.conf"
    }
  }'
```

## Configuration Management

### Environment-Specific Configs

Organize configs by environment:

```
configs/
  base.conf          # Common settings
  dev.conf           # Development overrides
  staging.conf       # Staging overrides
  production.conf    # Production settings
```

Use HOCON includes:
```json
# production.conf
include "base.conf"

spark {
  app-name = "Pipeline (Production)"
  config {
    "spark.executor.memory" = "8g"
  }
}
```

### Secrets Management

**Never commit secrets to config files.** Use environment variables:

```json
# pipeline.conf
pipeline {
  pipeline-components = [
    {
      instance-type = "com.mycompany.DatabaseLoader"
      instance-config {
        jdbc-url = ${JDBC_URL}
        username = ${DB_USERNAME}
        password = ${DB_PASSWORD}
      }
    }
  ]
}
```

Or use secrets managers:
- **AWS:** Secrets Manager, SSM Parameter Store
- **Kubernetes:** Secrets mounted as files/env vars
- **HashiCorp Vault:** Via environment injection

## Monitoring Production Pipelines

### MetricsHooks with Micrometer

```scala
import io.github.dwsmith1983.spark.pipeline.config.MetricsHooks
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry

val registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
val hooks = MetricsHooks(registry)

runner.run(config, hooks)

// Expose metrics endpoint for Prometheus scraping
```

### AuditHooks for Compliance

```scala
import io.github.dwsmith1983.spark.pipeline.config.audit._

val auditSink = new FileAuditSink(Paths.get("/var/log/pipeline-audit.jsonl"))
val hooks = AuditHooks(auditSink)

runner.run(config, hooks)
```

### Logging Best Practices

Configure Log4j2 for production:

```xml
<!-- log4j2.xml -->
<Configuration>
  <Appenders>
    <Console name="Console" target="SYSTEM_OUT">
      <JsonLayout compact="true" eventEol="true"/>
    </Console>
  </Appenders>
  <Loggers>
    <Logger name="io.github.dwsmith1983.spark.pipeline" level="INFO"/>
    <Root level="WARN">
      <AppenderRef ref="Console"/>
    </Root>
  </Loggers>
</Configuration>
```

## Health Checks and Alerting

### Dry-Run Validation

Before deploying config changes, validate with dry-run:

```scala
val result = runner.dryRun(config)
result match {
  case DryRunResult.Valid(components) =>
    println(s"Config valid: ${components.size} components")
  case DryRunResult.Invalid(errors) =>
    errors.foreach(e => println(s"Error: ${e.message}"))
    System.exit(1)
}
```

### Exit Codes

SimplePipelineRunner returns appropriate exit codes:

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Pipeline failure |
| 2 | Configuration error |

Use these for alerting in orchestration tools (Airflow, Argo, etc.).

## Next Steps

- [Scope & Design](./scope) - Understand framework capabilities and limitations
- [Lifecycle Hooks](./hooks) - Production monitoring with MetricsHooks and AuditHooks
- [Configuration](./configuration) - Advanced HOCON configuration patterns
