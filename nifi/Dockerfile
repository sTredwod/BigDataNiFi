FROM apache/nifi:1.28.1
USER root
RUN mkdir -p /opt/nifi/drivers
ADD --chown=nifi:nifi https://jdbc.postgresql.org/download/postgresql-42.7.14.jar /opt/nifi/drivers/postgresql-42.7.14.jar
USER nifi
