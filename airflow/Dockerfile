FROM apache/airflow:2.9.1-python3.11 as base
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip 
USER airflow
USER root
RUN apt update
RUN apt install git -y

RUN apt-get update
RUN apt-get install -y --no-install-recommends build-essential gcc wget


EXPOSE 8080
EXPOSE 5000

ENV AWS_ACCESS_KEY_ID 1
ENV AWS_SECRET_ACCESS_KEY 1
ENV AWS_DEFAULT_REGION us-west-1


USER airflow
RUN pip install -r /requirements.txt --no-cache-dir