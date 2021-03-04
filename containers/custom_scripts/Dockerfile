FROM continuumio/miniconda:4.6.14

# Install procps so that Nextflow can poll CPU usage
RUN apt-get update && apt-get install -y procps && apt-get clean -y

ADD ./bin/bash_script.sh /usr/local/bin/
ADD ./bin/python_script.py /usr/local/bin/
ADD ./bin/r_script.R /usr/local/bin/

RUN chmod +x /usr/local/bin/*.sh
RUN chmod +x /usr/local/bin/*.py
RUN chmod +x /usr/local/bin/*.R
