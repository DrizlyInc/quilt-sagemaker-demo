FROM python:3.7

# set the working directory
#RUN ["mkdir", "app"]
COPY run.sh .
WORKDIR app

# install code dependencies
COPY requirements.txt .
RUN ["pip", "install", "-r", "requirements.txt"]

# install environment dependencies
COPY app/ .
#COPY "app/eta" eta/
#COPY health-check-data.csv .

# provision environment
# todo change flask location?
ENV FLASK_APP app/app.py
#RUN ["ls"]
RUN ["chmod", "+x", "/run.sh"]
EXPOSE 8080
WORKDIR /
ENTRYPOINT ["/bin/bash", "/run.sh"]
CMD ["build"]
