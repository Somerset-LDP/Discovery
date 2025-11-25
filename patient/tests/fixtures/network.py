from typing import Generator

import pytest
from testcontainers.core.network import Network
import docker

@pytest.fixture(scope="session")
def docker_network() -> Generator[Network, None, None]:
    """Create a Docker network for containers to communicate."""
    network_name = "ldp"
    client = docker.from_env()

    # Check if the network already exists
    existing = [n for n in client.networks.list(names=[network_name])]
    if not existing:
        network = Network()
        network.name = network_name
        network.create()
    else:
        # Reuse the existing network
        network = Network()
        network.name = network_name
        network._network = existing[0]

    yield network

    # Remove only if we created it
    if not existing:
        network.remove()