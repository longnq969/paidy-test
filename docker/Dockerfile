FROM prefecthq/prefect:2.1.1-python3.8
RUN apt update
RUN apt install -y wget ca-certificates
RUN mkdir /app
COPY ./paidy_dse /app
COPY ./docker/mc /app
COPY ./docker/entry.sh /app
WORKDIR /app
RUN chmod +x ./entry.sh
ENV PYTHONPATH=${PYTHONPATH}:${PWD}
RUN pip3 install poetry
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev

