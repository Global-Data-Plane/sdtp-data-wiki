# syntax=docker/dockerfile:1

FROM python:3.8

WORKDIR /python-docker

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .
ENV FLASK_APP /python-docker/app.py

ENTRYPOINT FLASK_APP=/python-docker/app.py flask run --host=0.0.0.0 --port=8080