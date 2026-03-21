FROM python:3.11-slim

# Dependencies (note wget, unzip and zip are build-time dependencies only and will be removed at the end of the Dockerfile)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jdk wget unzip zip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /synthetic-data-generator

# Synthea setup - download Synthea jar and modify it with localised resources and properties
RUN wget https://github.com/synthetichealth/synthea/releases/download/v3.4.0/synthea-with-dependencies.jar
RUN mkdir -p /tmp/synthea-jar && unzip synthea-with-dependencies.jar -d /tmp/synthea-jar && rm synthea-with-dependencies.jar

COPY synthea/england/resources /tmp/synthea-jar/

# US specific data that is not needed for England. Removed to prevent runtime errors
RUN rm -rf /tmp/synthea-jar/payers/eligibility_input_files

# Append localised synthea.properties 
COPY synthea/england/synthea.properties /tmp/synthea.properties
RUN echo "# === England Profile Overrides ===" >> /tmp/synthea-jar/synthea.properties && \
    cat /tmp/synthea.properties >> /tmp/synthea-jar/synthea.properties && \
    rm /tmp/synthea.properties

RUN (cd /tmp/synthea-jar && zip -r /synthetic-data-generator/synthea-with-dependencies.jar .) && rm -rf /tmp/synthea-jar    

# Generator setup
COPY *.py .
RUN chmod +x *.py 
COPY postcodes postcodes 

# Non-root user setup
RUN useradd -m appuser
RUN chown -R appuser:appuser /synthetic-data-generator

# Tidy up - remove build-time utilities
RUN apt-get remove -y wget unzip zip && apt-get autoremove -y && apt-get clean && rm -rf /var/lib/apt/lists/*

USER appuser

ENTRYPOINT ["python", "generate.py"]
CMD ["--help"]
