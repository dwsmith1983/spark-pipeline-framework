// Resolve plugin dependency conflicts
ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always

// Cross-compilation matrix support
addSbtPlugin("com.eed3si9n" % "sbt-projectmatrix" % "0.9.1")

// Assembly for fat JARs (optional, for runner)
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.5")

// Scalafmt for code formatting
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")

// Scalastyle for static code analysis
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

// Code coverage
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.12")
