ARG BASE_IMAGE=nansencenter/geospaas
FROM ${BASE_IMAGE} as base

RUN pip install --no-cache-dir \
    celery==4.4 \
    django-celery-results==1.2 \
    paramiko==2.7.1 \
    redis==3.5 \
    scp==0.13.2

FROM base as full

WORKDIR /tmp/setup
COPY setup.py README.md ./
COPY geospaas_processing ./geospaas_processing
RUN python setup.py bdist_wheel && \
    pip install -v dist/geospaas_processing-*.whl && \
    cd /tmp && rm -rf /tmp/setup/
WORKDIR /

ENTRYPOINT ["celery"]
CMD ["worker", "-A", "geospaas_processing.tasks", "-l", "info"]
