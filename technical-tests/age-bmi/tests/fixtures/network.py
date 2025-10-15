from typing import Generator

import pytest
from testcontainers.core.network import Network

@pytest.fixture(scope="session")
def docker_network() -> Generator[Network, None, None]:
    """Create a Docker network for containers to communicate."""
    network = Network()
    network.name = "ldp"
    network.create()
    
    yield network
    
    network.remove()