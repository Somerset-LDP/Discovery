FROM public.ecr.aws/lambda/python:3.12

# Set working directory
WORKDIR ${LAMBDA_TASK_ROOT}

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code maintaining directory structure
COPY common/ ./common/
COPY pipeline/gp_pipeline.py ./pipeline/
COPY aws/lambda/gp/handler.py ./

# Set the Lambda handler
CMD ["handler.lambda_handler"]