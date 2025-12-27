// Resolve plugin dependency conflicts
ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always

// Cross-compilation matrix support
addSbtPlugin("com.eed3si9n" % "sbt-projectmatrix" % "0.9.1")

// Assembly for fat JARs (optional, for runner)
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.5")

// =============================================================================
// Code Quality Tools - Each has a specific responsibility:
//
//   Scalafmt    → Formatting (whitespace, line breaks, import order)
//   Scalastyle  → Style rules (naming, complexity, method length)
//   Scalafix    → Semantic linting (unused code, dangerous patterns)
//   OWASP       → Security (dependency vulnerability scanning)
//
// See .scalafmt.conf, scalastyle-config.xml, .scalafix.conf for configs.
// =============================================================================

// Scalafmt - Code formatting (OWNS: whitespace, indentation, import ordering)
// Run: sbt scalafmtAll (format) or sbt scalafmtCheckAll (check)
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")

// Scalastyle - Style linting (OWNS: naming conventions, complexity, method length)
// Run: sbt scalastyle
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

// Scalafix - Semantic linting (OWNS: unused imports/vars, dangerous syntax)
// Run: sbt "scalafix RemoveUnused" or sbt "scalafixAll"
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.11.1")

// OWASP Dependency Check - Security vulnerability scanning
// Run: sbt dependencyCheck (scans against CVE database)
// Requires NVD API key: https://nvd.nist.gov/developers/request-an-api-key
addSbtPlugin("net.nmoncho" % "sbt-dependency-check" % "1.8.4")

// Code coverage
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.12")

// Maven Central publishing
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.12.2")
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.2.1")
