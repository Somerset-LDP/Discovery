#!/bin/bash
# filepath: ethnicity/bin/start.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
TERMINOLOGY_SERVER_DIR="$PROJECT_ROOT/terminology-server"
TERMINOLOGY_RESOURCES_DIR="$PROJECT_ROOT/terminology-resources"
FHIR_BASE_URL="http://localhost:8080/fhir/"
TIMEOUT=60  # seconds

upload_resources() {
  local resource_dir="$1"
  local resource_type="$2"
  local endpoint="${FHIR_BASE_URL}${resource_type}"

  echo "Uploading ${resource_type} resources from $resource_dir..."
  for file in "$resource_dir"/*.json; do
    if [ -f "$file" ]; then
      echo "Uploading $(basename "$file") to $endpoint"
      RESPONSE=$(curl -s -w "\nHTTP_STATUS:%{http_code}\n" -X POST \
        -H "Content-Type: application/fhir+json; charset=UTF-8" \
        --data-binary @"$file" \
        "$endpoint")
      echo "$RESPONSE"
      STATUS=$(echo "$RESPONSE" | grep HTTP_STATUS | cut -d: -f2)
      if [ "$STATUS" -ge 400 ]; then
        echo "ERROR: Failed to upload $(basename "$file")"
      fi
    fi
  done
}

echo "Loading FHIR resources from $TERMINOLOGY_RESOURCES_DIR..."

upload_resources "$TERMINOLOGY_RESOURCES_DIR/codesystem" "CodeSystem"
upload_resources "$TERMINOLOGY_RESOURCES_DIR/valueset" "ValueSet"
upload_resources "$TERMINOLOGY_RESOURCES_DIR/conceptmap" "ConceptMap"

echo "All resources processed."