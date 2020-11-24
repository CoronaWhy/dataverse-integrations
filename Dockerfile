FROM python:3.10.0a2-buster
COPY ./syncDataverse /syncDataverse
RUN pip install -r /syncDataverse/requirements.txt 
