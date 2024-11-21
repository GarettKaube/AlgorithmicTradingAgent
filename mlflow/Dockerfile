FROM python:3.11.9-slim

WORKDIR /mlflow

RUN pip install mlflow
USER root
RUN apt update
RUN apt install git -y


ENV BACKEND_URI sqlite:///mlflow.db


EXPOSE 5000

CMD mlflow server --backend-store-uri $BACKEND_URI --host 0.0.0.0 --port 5000