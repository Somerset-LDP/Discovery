FROM public.ecr.aws/lambda/python:3.12

# Set working directory
WORKDIR ${LAMBDA_TASK_ROOT}

# Dependency installation
COPY requirements.txt .

RUN --mount=type=secret,id=ssl-certs \
    if [ -f /run/secrets/ssl-certs ]; then \
        PIP_CERT=/run/secrets/ssl-certs; \
    else \
        PIP_CERT=""; \
    fi && \
    echo "Using cert: $PIP_CERT" && \
    pip install ${PIP_CERT:+--cert $PIP_CERT} --no-cache-dir -r requirements.txt

# Copy matching module
COPY matching/__init__.py ./matching/
COPY matching/service.py ./matching/
COPY matching/patient.py ./matching/

# Copy mpi module
COPY mpi/__init__.py ./mpi/
COPY mpi/matching.py ./mpi/
COPY mpi/local/repository.py ./mpi/local/
COPY mpi/local/matching.py ./mpi/local/
COPY mpi/pds/asynchronous/request/__init__.py ./mpi/pds/asynchronous/request/
COPY mpi/pds/asynchronous/request/server.py ./mpi/pds/asynchronous/request/
COPY mpi/pds/asynchronous/request/trace_status.py ./mpi/pds/asynchronous/request/

# Copy the Lambda handler
COPY matching/aws/lambda/handler.py .

# Set the Lambda handler
CMD ["handler.lambda_handler"]
