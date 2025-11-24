FROM public.ecr.aws/lambda/python:3.12

# Set working directory
WORKDIR ${LAMBDA_TASK_ROOT}

# Dependency installation
COPY patient/requirements.txt .

RUN --mount=type=secret,id=ssl-certs \
    if [ -f /run/secrets/ssl-certs ]; then \
        PIP_CERT=/run/secrets/ssl-certs; \
    else \
        PIP_CERT=""; \
    fi && \
    echo "Using cert: $PIP_CERT" && \
    pip install ${PIP_CERT:+--cert $PIP_CERT} --no-cache-dir -r requirements.txt

# Copy matching module
COPY patient/matching/__init__.py ./matching/
COPY patient/matching/service.py ./matching/
COPY patient/matching/patient.py ./matching/
COPY patient/matching/matchers.py ./matching/

# Copy mpi module
COPY patient/mpi/__init__.py ./mpi/
COPY patient/mpi/matching.py ./mpi/
COPY patient/mpi/local/repository.py ./mpi/local/
COPY patient/mpi/local/matching.py ./mpi/local/
COPY patient/mpi/pds/asynchronous/request/__init__.py ./mpi/pds/asynchronous/request/
COPY patient/mpi/pds/asynchronous/request/client.py ./mpi/pds/asynchronous/request/

# Copy the Lambda handler
COPY patient/matching/aws/lambda/handler.py .

# Set the Lambda handler
CMD ["handler.lambda_handler"]