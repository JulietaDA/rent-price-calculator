FROM apache/airflow:2.10.2-python3.10

# copy requirements file to the docker image
COPY requirements.txt /requirements.txt

# install python packages from requirements file
RUN pip install --no-cache-dir "apache-airflow==2.10.2" -r /requirements.txt