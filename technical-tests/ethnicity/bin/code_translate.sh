#!/bin/bash

# Usage: ./translate_code.sh <source-system> <code> <conceptmap-canonical-url>
# Example:
# ./translate_code.sh "https://nhssomerset.nhs.uk/ldp/codesystem/nhs_data_dictionary" "A" "https://nhssomerset.nhs.uk/ldp/conceptmap/nhs_2001a_ethnicity_to_ons_2021_ethnicity"

FHIR_BASE_URL="http://localhost:8080/fhir"

if [ $# -ne 2 ]; then
  echo "Usage: $0 <source-system> <code>"
  exit 1
fi

CODE="$1"
SOURCE_SYSTEM="$2"

# URL-encode parameters
ENCODED_SYSTEM=$(python3 -c "import urllib.parse; print(urllib.parse.quote('''$SOURCE_SYSTEM'''))")
ENCODED_CODE=$(python3 -c "import urllib.parse; print(urllib.parse.quote('''$CODE'''))")

# Construct $translate URL
TRANSLATE_URL="$FHIR_BASE_URL/ConceptMap/\$translate?code=$ENCODED_CODE&system=$ENCODED_SYSTEM"

# Print the curl command
echo "Issuing curl command:"
echo "curl -v -X GET \"$TRANSLATE_URL\" -H 'Accept: application/fhir+json'"
echo "---------------------------------"

# Issue the curl command
RESPONSE=$(curl -s -w "\n%{http_code}" -X GET "$TRANSLATE_URL" -H 'Accept: application/fhir+json')

# Split response and status
HTTP_BODY=$(echo "$RESPONSE" | sed '$d')
HTTP_STATUS=$(echo "$RESPONSE" | tail -n1)

echo "HTTP Status: $HTTP_STATUS"
echo "Response Body:"
echo "$HTTP_BODY"

# Basic error handling
if [ "$HTTP_STATUS" -ne 200 ]; then
  echo "Error: Failed to translate code. Check FHIR server, code, system, and ConceptMap URL."
  exit 2
fi
