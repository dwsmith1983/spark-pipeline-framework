# Security Policy

## Dependency Vulnerability Scanning

This project uses [OWASP Dependency Check](https://owasp.org/www-project-dependency-check/) to scan for known CVEs in dependencies. The scan runs on every pull request.

## Suppressed CVEs (False Positives)

The following CVEs are suppressed in `dependency-check-suppressions.xml` because they are false positives or not exploitable in this context:

| CVE | Reason |
|-----|--------|
| CVE-2018-17190 | False positive: This 2018 Spark CVE was fixed in Spark 2.4.0. We use Spark 3.5.x/4.0.x |
| CVE-2017-1000034 | False positive: Akka CVE incorrectly matched to scala-parallel-collections |
| CVE-2023-37475 | Not exploitable: Avro ReflectData SSRF requires untrusted schema input |
| jQuery CVEs | Not exploitable: XSS vulnerabilities require web browser context |

## Known Transitive CVEs (From Spark/Hadoop)

The following CVEs are in **transitive dependencies from Apache Spark and Hadoop**. These are inherited from the Spark ecosystem and cannot be resolved by this project directly.

### Netty (Transitive from Spark)

| CVE | Severity | Component | Notes |
|-----|----------|-----------|-------|
| CVE-2025-58057 | High | netty 4.1.118 | Recent CVE in Spark's shuffle/RPC layer |
| CVE-2025-55163 | High | netty 4.1.118 | Requires network exposure |
| CVE-2025-58056 | High | netty 4.1.118 | Requires network exposure |

**Mitigation**: Spark manages Netty internally for shuffle. Ensure cluster network is secured.

### Jetty (Shaded in hadoop-client-runtime)

| CVE | Severity | Component | Notes |
|-----|----------|-----------|-------|
| CVE-2024-8184 | Medium | jetty 9.4.53 | Bundled in Hadoop client JAR |
| CVE-2024-22201 | High | jetty 9.4.53 | HTTP/2 vulnerability |
| CVE-2024-6763 | Medium | jetty 9.4.53 | Session handling |
| CVE-2024-13009 | Medium | jetty 9.4.53 | |
| CVE-2024-9823 | Medium | jetty 9.4.53 | |

**Mitigation**: Hadoop shades Jetty for internal use. Not exposed to end users.

### Janino (Transitive from Spark SQL)

| CVE | Severity | Component | Notes |
|-----|----------|-----------|-------|
| CVE-2023-33546 | High | janino 3.1.9 | Code execution via crafted expressions |

**Mitigation**: Requires untrusted SQL input to Spark SQL codegen. Validate SQL inputs.

### ZooKeeper (Transitive from Spark)

| CVE | Severity | Component | Notes |
|-----|----------|-----------|-------|
| CVE-2025-58457 | High | zookeeper 3.9.3 | Recent CVE |

**Mitigation**: ZooKeeper is optional (used in cluster mode). Secure ZK ensemble if used.

### JLine (Shaded in hadoop-client-runtime)

| CVE | Severity | Component | Notes |
|-----|----------|-----------|-------|
| CVE-2023-50572 | Medium | jline 3.9.0 | Terminal input handling |

**Mitigation**: Bundled in Hadoop for CLI. Not exposed in batch pipelines.

### Hive Storage API (Transitive from Spark)

| CVE | Severity | Component | Notes |
|-----|----------|-----------|-------|
| CVE-2024-29869 | Medium | hive-storage-api 2.8.1 | |
| CVE-2021-34538 | High | hive-storage-api 2.8.1 | |
| CVE-2024-23953 | Medium | hive-storage-api 2.8.1 | |
| CVE-2024-23945 | Medium | hive-storage-api 2.8.1 | |

**Mitigation**: Used for ORC file format support. Validate input data sources.

### Commons Libraries (Shaded in hadoop-client-runtime)

| CVE | Severity | Component | Notes |
|-----|----------|-----------|-------|
| CVE-2025-48924 | Medium | commons-lang3 3.12.0/3.17.0 | |
| CVE-2025-48734 | Medium | commons-beanutils 1.9.4 | |
| CVE-2023-35116 | Low | jackson-databind 2.12.7.1 | |

**Mitigation**: Shaded in Hadoop JAR. Not directly accessible.

## Reporting Security Issues

If you discover a security vulnerability in this project (not transitive dependencies), please report it by opening a GitHub issue or contacting the maintainers directly.

For vulnerabilities in Apache Spark, Hadoop, or other upstream projects, please report to the respective Apache Security teams.

## Updates

This document was last updated: 2025-12-27
