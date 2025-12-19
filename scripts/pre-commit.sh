#!/usr/bin/env bash
#
# Pre-commit hook for spark-pipeline-framework
# Runs scalafmt and scalastyle checks before committing
#
# Installation:
#   Option 1: Copy to .git/hooks/pre-commit
#     cp scripts/pre-commit.sh .git/hooks/pre-commit
#     chmod +x .git/hooks/pre-commit
#
#   Option 2: Use pre-commit framework (requires Python)
#     pip install pre-commit
#     pre-commit install
#

set -e

echo "Running pre-commit checks..."

# Check if sbt is available
if ! command -v sbt &> /dev/null; then
    echo "Error: sbt is not installed or not in PATH"
    exit 1
fi

# Run scalafmt check
echo "Checking formatting with scalafmt..."
if ! sbt --error scalafmtCheckAll 2>&1; then
    echo ""
    echo "Scalafmt check failed. Run 'sbt scalafmtAll' to fix formatting."
    exit 1
fi

# Run scalastyle check
echo "Checking style with scalastyle..."
if ! sbt --error scalastyle 2>&1; then
    echo ""
    echo "Scalastyle check failed. Please fix the style issues above."
    exit 1
fi

echo "All pre-commit checks passed!"
