FROM eoscprominence/htcondor:8.9.5

# Dependencies
RUN yum -y update && \
    yum -y install epel-release && \
    yum -y install singularity \
                   wget \
                   unzip \
                   python-pip \
                   python-devel \
                   gcc \
                   git \
                   openssl \
                   openssh-server

# Python requests
RUN pip install requests

# Jobs will be done as this user
RUN useradd user
 
# Install udocker
RUN pip install git+https://github.com/indigo-dc/udocker && \
    ln -s /usr/bin/udocker /usr/local/bin/udocker && \
    su user -c "udocker install"

# Install Oneclient
RUN curl -sS  http://get.onedata.org/oneclient.sh | bash

# Configuration
COPY 00-ports /etc/condor/config.d/
COPY 10-worker /etc/condor/config.d/
COPY 11-health /etc/condor/config.d/
COPY 22-security /etc/condor/config.d/

# Scripts
COPY condor_url_fetch /usr/local/libexec/
COPY job-prepare-hook /usr/local/bin/
COPY get-location /usr/local/bin/
COPY healthcheck.py  /usr/local/bin/
COPY write-resources.py /usr/local/bin/

# Entrypoint
COPY docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
