FROM jupyter/datascience-notebook

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

USER root

RUN apt-get -y update && \
        apt-get install -y --no-install-recommends \
        gcc \
        build-essential \
        libpq-dev \
        liblapack-dev \
        git \
        graphviz

RUN pip install --upgrade pip

COPY requirements.txt .

RUN pip install -r requirements.txt
