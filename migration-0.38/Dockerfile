FROM python:3.10-slim-buster

# Add Maintainer Info
LABEL maintainer="signoz"

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

# run the binary
ENTRYPOINT [ "python3", "-u", "main.py"]