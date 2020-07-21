FROM python:3.7-buster
MAINTAINER Chair of Software Engineering, University of Wuerzburg

COPY ./app /app

RUN apt-get update && apt-get -y install curl
COPY ./requirements.txt requirements.txt
HEALTHCHECK --start-period=10s --interval=10s --timeout=5s --retries=3 CMD curl --fail http://localhost:5000/ || exit 1
RUN pip install -r requirements.txt
ENTRYPOINT [ "python", "./app/app.py" ]