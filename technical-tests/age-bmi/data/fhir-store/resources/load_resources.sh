#!/bin/bash
# filepath: data/fhir-store/resources/load_resources.sh

set -e

if [ -z "$1" ]; then
  echo "Usage: $0 <FHIR_BASE_URL>"
  exit 1
fi

FHIR_BASE_URL="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FHIR_STORE_DIR="$SCRIPT_DIR"
TIMEOUT=60  # seconds

resource_exists() {
  local resource_type="$1"
  local id="$2"
  local url="${FHIR_BASE_URL}/${resource_type}/$id"

  # Check existence by id only
  if [ -n "$id" ]; then
    STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$url")
    if [ "$STATUS" -eq 200 ]; then
      return 0
    fi
  fi

  return 1
}

upload_resources() {
  local resource_dir="$1"
  local resource_type="$2"
  local base_endpoint="${FHIR_BASE_URL}/${resource_type}"

  echo "Uploading ${resource_type} resources from $resource_dir..."
  for file in "$resource_dir"/*.json; do
    if [ -f "$file" ]; then
      # Extract id from the resource
      ID=$(jq -r '.id // empty' "$file")
      if resource_exists "$resource_type" "$ID"; then
        echo "Resource $(basename "$file") (id: $ID) already exists. Skipping."
        continue
      fi

      endpoint="$base_endpoint/$ID"
      echo "Uploading $(basename "$file") to $endpoint"
      RESPONSE=$(curl -s -w "\nHTTP_STATUS:%{http_code}\n" -X PUT \
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

echo "Loading FHIR resources from $FHIR_STORE_DIR..."

upload_resources "$FHIR_STORE_DIR/codesystem" "CodeSystem"
upload_resources "$FHIR_STORE_DIR/valueset" "ValueSet"
upload_resources "$FHIR_STORE_DIR/conceptmap" "ConceptMap"
upload_resources "$FHIR_STORE_DIR/observationdefinition" "ObservationDefinition"

echo "All resources processed."