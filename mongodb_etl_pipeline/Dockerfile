# using Python 3.8 as base image
FROM python:3.8-slim

# setting working directory
WORKDIR /app

# copy current directory contents into /app inside the container
COPY . /app

# install dependencies
RUN pip install -r requirements.txt

# run the app
CMD ["python", "etl_pipeline.py"]
