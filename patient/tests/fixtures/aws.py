from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import HttpWaitStrategy
import requests
from contextlib import contextmanager

@contextmanager
def create_lambda_container_with_env(docker_network, env_vars=None, image: str = "patient-matching:latest"):
    """
    Helper function to create a Lambda container with environment variables.
    
    Args:
        postgres_db: PostgreSQL database engine
        docker_network: Docker network for container communication
        
    Yields:
        running_container: Ready-to-use Lambda container
    """
    container = DockerContainer(image)
    container.with_exposed_ports(8080)

    # Add to the same network as PostgreSQL
    container.with_network(docker_network)

    # Default environment variables
    defaults = {
        "MPI_DB_USERNAME": "mpi_writer",
        "MPI_DB_PASSWORD": "DefaultPassword123!",
        "MPI_DB_HOST": "db",
        "MPI_DB_NAME": "ldp",
        "MPI_DB_PORT": "5432",
        "MPI_SCHEMA_NAME": "mpi"
    }

    # Use provided env_vars or defaults
    env = defaults.copy()
    if env_vars:
        env.update(env_vars)

    for key, value in env.items():
        container.with_env(key, value)    

    container.waiting_for(HttpWaitStrategy(8080, "/2015-03-31/functions/function/invocations").for_status_code_matching(lambda status_code: 200 <= status_code < 600))

    with container as running_container:        
        yield running_container

def invoke_lambda(container, event, timeout: int = 30):
    """Invoke Lambda function in container"""
    port = container.get_exposed_port(8080)
      
    url = f"http://localhost:{port}/2015-03-31/functions/function/invocations"
    
    response = requests.post(url, json=event, timeout=timeout)
    return response.json()