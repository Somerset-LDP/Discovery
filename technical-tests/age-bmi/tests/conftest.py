"""Pytest configuration and fixture registration."""

pytest_plugins = [
    "tests.fixtures.network",
    "tests.fixtures.postgres", 
    "tests.fixtures.fhir"
]