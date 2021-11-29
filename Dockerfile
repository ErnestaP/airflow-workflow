# FROM apache/airflow:2.2.1-python3.7
# USER root
# RUN pip install --upgrade apache-airflow-providers-google
# RUN apt-get update \
#   && apt-get install -y --no-install-recommends \
#          vim \
#   && apt-get autoremove -yqq --purge \
#   && apt-get clean \
#   && rm -rf /var/lib/apt/lists/*
# USER airflow

FROM apache/airflow:latest-python3.8

RUN export PYTHONPATH="/home/airflow/.local/lib/python3.8/site-packages"
USER root
RUN apt-get update && \
    apt-get -y install libleveldb-dev libssl-dev libkrb5-dev
RUN pip install --ignore-installed --requirement requirements.txt 
RUN pip install wheel 
RUN python setup.py bdist_wheel 
RUN pip install plyvel 
WORKDIR /code
COPY . .
WORKDIR /code/airflow-workflow
RUN git clone https://github.com/SCOAP3/hepcrawl.git
WORKDIR /code/airflow-workflow/hepcrawl
RUN python setup.py bdist_wheel
RUN python setup.py install



