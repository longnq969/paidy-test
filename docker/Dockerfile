FROM prefecthq/prefect:2.1.1-python3.8

RUN mkdir /app
COPY ./paidy_dse /app
WORKDIR /app
ENV PYTHONPATH=${PYTHONPATH}:${PWD}
RUN apt-get install ca-certificates
RUN pip3 install poetry
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev
