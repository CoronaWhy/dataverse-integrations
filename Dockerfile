FROM python:3.7


RUN mkdir ./syncDataverse
WORKDIR /syncDataverse
COPY ./syncDataverse syncDataverse

RUN pip install -r ./syncDataverse/requirements.txt
