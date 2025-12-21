#!/bin/bash
# Inject navigation back to main docs into all ScalaDoc HTML files
# This script is run during CI after ScalaDoc generation

set -e

API_DIR="${1:-website/static/api}"

if [ ! -d "$API_DIR" ]; then
  echo "Error: API directory not found: $API_DIR"
  exit 1
fi

# Create a temporary file with the CSS to inject
CSS_FILE=$(mktemp)
cat > "$CSS_FILE" << 'EOF'
<style>
.docs-nav-banner {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
  padding: 8px 16px;
  z-index: 10000;
  display: flex;
  align-items: center;
  gap: 16px;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  box-shadow: 0 2px 8px rgba(0,0,0,0.3);
}
.docs-nav-banner a {
  color: #fff;
  text-decoration: none;
  font-size: 14px;
  padding: 6px 12px;
  border-radius: 4px;
  transition: background 0.2s;
}
.docs-nav-banner a:hover {
  background: rgba(255,255,255,0.1);
}
.docs-nav-banner .nav-title {
  color: #58a6ff;
  font-weight: 600;
  font-size: 14px;
}
.docs-nav-banner .nav-sep {
  color: #666;
}
body { padding-top: 44px !important; }
#search { top: 44px !important; }
</style>
EOF

# Create a temporary file with the HTML to inject
HTML_FILE=$(mktemp)
cat > "$HTML_FILE" << 'EOF'
<div class="docs-nav-banner"><a href="/spark-pipeline-framework/" class="nav-title">Spark Pipeline Framework</a><span class="nav-sep">|</span><a href="/spark-pipeline-framework/docs/getting-started">Docs</a><a href="/spark-pipeline-framework/docs/components">Components</a><a href="/spark-pipeline-framework/docs/hooks">Hooks</a></div>
EOF

# Read the content into variables
CSS_CONTENT=$(cat "$CSS_FILE")
HTML_CONTENT=$(cat "$HTML_FILE")

count=0

# Process each HTML file
find "$API_DIR" -name "*.html" -type f | while read -r file; do
  # Skip if already injected
  if grep -q "docs-nav-banner" "$file" 2>/dev/null; then
    continue
  fi

  # Create a temp file for the modified content
  tmp_file=$(mktemp)

  # Use awk to inject CSS before </head> and HTML after <body...>
  awk -v css="$CSS_CONTENT" -v html="$HTML_CONTENT" '
    {
      # Inject CSS before </head>
      gsub(/<\/head>/, css "</head>")
      # Inject HTML after <body> (simple case)
      gsub(/<body>/, "<body>" html)
      # Inject HTML after <body ...> (with attributes)
      if (/<body [^>]*>/) {
        match($0, /<body [^>]*>/)
        before = substr($0, 1, RSTART + RLENGTH - 1)
        after = substr($0, RSTART + RLENGTH)
        $0 = before html after
      }
      print
    }
  ' "$file" > "$tmp_file"

  # Replace original with modified
  mv "$tmp_file" "$file"

  echo "Processed: $file"
  ((count++)) || true
done

# Cleanup temp files
rm -f "$CSS_FILE" "$HTML_FILE"

echo "Done! Injected navigation into ScalaDoc HTML files in $API_DIR"
