#!/usr/bin/env python3
"""
Script to generate encrypted data keys for pseudonymisation.

This script uses AWS KMS to generate a new AES-256 data key and returns
the encrypted version (CiphertextBlob) that should be stored in Secrets Manager.

Usage:
    python generate_encrypted_key.py --kms-key-id <KMS_KEY_ID> [--version <VERSION>]

Example:
    python generate_encrypted_key.py --kms-key-id arn:aws:kms:eu-west-2:123456789:key/abcd-1234
    python generate_encrypted_key.py --kms-key-id alias/pseudonymisation-key --version v2 --add-to-existing
"""

import argparse
import base64
import json
import sys

import boto3
from botocore.exceptions import ClientError


def generate_encrypted_key(kms_key_id: str, version: str = "v1") -> dict:
    kms_client = boto3.client('kms')

    try:
        print(f"Generating new data key using KMS key: {kms_key_id}")
        response = kms_client.generate_data_key(
            KeyId=kms_key_id,
            KeySpec='AES_256'
        )

        encrypted_key = response['CiphertextBlob']
        encrypted_key_b64 = base64.b64encode(encrypted_key).decode('ascii')
        print(f"\n Successfully generated encrypted data key for version '{version}'")

        return {
            'version': version,
            'encrypted_key_base64': encrypted_key_b64
        }

    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        print(f"\n✗ Error generating data key: {error_code}")
        print(f"  {e}")
        sys.exit(1)


def create_secrets_manager_structure(keys_data: list) -> dict:
    if not keys_data:
        raise ValueError("Must provide at least one key")

    current_version = keys_data[-1]['version']
    keys_dict = {
        item['version']: item['encrypted_key_base64']
        for item in keys_data
    }

    return {
        'current': current_version,
        'keys': keys_dict
    }


def main():
    parser = argparse.ArgumentParser(
        description='Generate encrypted data key for pseudonymisation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        '--kms-key-id',
        required=True,
        help='KMS key ID or ARN (e.g., arn:aws:kms:region:account:key/xxx or alias/my-key)'
    )
    parser.add_argument(
        '--version',
        default='v1',
        help='Version label for this key (default: v1)'
    )
    parser.add_argument(
        '--add-to-existing',
        action='store_true',
        help='Show how to add this key to existing structure'
    )
    args = parser.parse_args()

    result = generate_encrypted_key(args.kms_key_id, args.version)
    if args.add_to_existing:
        print("\nAdd this to your existing 'keys' object in Secrets Manager:")
        print(f'"{result["version"]}": "{result["encrypted_key_base64"]}"')

        print("\nThen update 'current' to use this version:")
        print(f'"current": "{result["version"]}"')
    else:
        structure = create_secrets_manager_structure([result])

        print("\nStore this in AWS Secrets Manager as 'pseudonymisation/key-versions':")
        print(json.dumps(structure, indent=2))


if __name__ == '__main__':
    main()

