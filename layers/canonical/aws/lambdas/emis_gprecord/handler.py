import os
import json
import logging
import socket
import psycopg2  # PostgreSQL driver
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_db_credentials(secret_name):
    """AWS Secrets Manager data fetch"""
    client = boto3.client('secretsmanager')
    try:
        response = client.get_secret_value(SecretId=secret_name)
        secret = response['SecretString']
        return secret
    except Exception as e:
        logger.error(f"Error retrieving secret: {e}")
        raise

def test_dns(host):
    """DNS test"""
    try:
        ip = socket.gethostbyname(host)
        logger.info(f"DNS resolution for {host}: {ip}")
        return ip
    except Exception as e:
        logger.error(f"DNS resolution failed for {host}: {e}")
        return None

def test_port(host, port):
    """Port availability"""
    try:
        sock = socket.create_connection((host, port), timeout=3)
        sock.close()
        logger.info(f"Port {port} on {host} is reachable")
        return True
    except Exception as e:
        logger.error(f"Port test failed: {e}")
        return False

def lambda_handler(event, context):
    secret_name = os.environ.get('DB_SECRET_NAME')
    logger.info("test")
    if not secret_name:
        return {"status": "error", "details": "Missing DB_SECRET_NAME env variable"}

    db_secret = get_db_credentials(secret_name)
    host = 'somerset-dev-canonical-postgres-db.cze66q2qw2vu.eu-west-2.rds.amazonaws.com'
    user = 'canonical_writer'
    password = db_secret
    dbname = 'canonical'
    port = 5432

    # Test DNS
    ip = test_dns(host)
    if not ip:
        return {"status": "error", "details": "DNS resolution failed"}

    # Test port
    if not test_port(ip, port):
        return {"status": "error", "details": f"Cannot reach {host}:{port}"}

    # Próba połączenia z DB
    try:
        conn = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            dbname=dbname,
            port=port,
            connect_timeout=5
        )
        logger.info("DB connection successful")
        conn.close()
        return {"status": "success", "details": f"Connected to {host}"}
    except Exception as e:
        logger.error(f"DB connection failed: {e}")
