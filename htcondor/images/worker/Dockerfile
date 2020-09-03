FROM centos:7

# Install HTCondor
RUN yum -y update && \
    yum -y install wget \
                   epel-release

RUN cd /etc/yum.repos.d && \
    wget http://research.cs.wisc.edu/htcondor/yum/repo.d/htcondor-development-rhel7.repo

RUN wget http://research.cs.wisc.edu/htcondor/yum/RPM-GPG-KEY-HTCondor && \
    rpm --import RPM-GPG-KEY-HTCondor && \
    rm RPM-GPG-KEY-HTCondor

RUN yum -y install condor

# Dependencies
RUN yum -y install singularity \
                   unzip \
                   bzip2 \
                   python-pip \
                   python-devel \
                   gcc \
                   git \
                   openssl \
                   openssh-server

# Jobs will be run as this user
RUN useradd user

# Python requests
RUN pip install requests
 
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
