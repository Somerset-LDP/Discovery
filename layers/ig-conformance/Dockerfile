FROM public.ecr.aws/lambda/python:3.12

# Set working directory
WORKDIR ${LAMBDA_TASK_ROOT}

COPY requirements.txt .

RUN --mount=type=secret,id=ssl-certs \
    if [ -f /run/secrets/ssl-certs ]; then \
        PIP_CERT=/run/secrets/ssl-certs; \
    else \
        PIP_CERT=""; \
    fi && \
    echo "Using cert: $PIP_CERT" && \
    pip install ${PIP_CERT:+--cert $PIP_CERT} --no-cache-dir -r requirements.txt

# Copy source code maintaining directory structure
COPY common/ ./common/
COPY pipeline/ ./pipeline/
COPY aws/lambdas/handler.py ./handler.py

# Set the Lambda handler
CMD ["handler.lambda_handler"]
