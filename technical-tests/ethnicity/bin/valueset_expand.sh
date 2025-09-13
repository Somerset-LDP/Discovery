#!/bin/bash

# Usage: ./expand_valueset.sh <canonical-url>
# Example:
# ./expand_valueset.sh "https://nhssomerset.nhs.uk/ldp/valueset/nhs_data_dictionary_ethnicity_2001a"

FHIR_BASE_URL="http://localhost:8080/fhir"

if [ -z "$1" ]; then
  echo "Error: Please provide a canonical ValueSet URL."
  exit 1
fi

CANONICAL_URL="$1"

# URL-encode the canonical URL
ENCODED_URL=$(python3 -c "import urllib.parse; print(urllib.parse.quote('''$CANONICAL_URL'''))")

# Construct the full $expand URL
EXPAND_URL="$FHIR_BASE_URL/ValueSet/\$expand?url=$ENCODED_URL"

# Print the curl command being issued
echo "Issuing curl command:"
echo "curl -v -X GET \"$EXPAND_URL\" -H 'Accept: application/fhir+json'"
echo "---------------------------------"

# Issue the curl command
RESPONSE=$(curl -s -w "\n%{http_code}" -X GET "$EXPAND_URL" -H 'Accept: application/fhir+json')
# Split response and status
HTTP_BODY=$(echo "$RESPONSE" | sed '$d')
HTTP_STATUS=$(echo "$RESPONSE" | tail -n1)

echo "HTTP Status: $HTTP_STATUS"
echo "Response Body:"
echo "$HTTP_BODY"

# Basic error handling
if [ "$HTTP_STATUS" -ne 200 ]; then
  echo "Error: Failed to expand ValueSet. Check FHIR server availability and canonical URL."
  exit 2
fi
