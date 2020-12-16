# Build oneclient 20.02
FROM centos:7 AS oneclient2002
RUN curl -sS http://get.onedata.org/oneclient-2002.sh | bash
RUN cd /opt && \
    tar czf oneclient-2002.tgz oneclient

# Build oneclient 19.02
FROM centos:7 AS oneclient1902
RUN curl -sS http://get.onedata.org/oneclient-1902.sh | bash
RUN cd /opt && \
    tar czf oneclient-1902.tgz oneclient

# Build oneclient 18.02
FROM centos:7 AS oneclient1802
RUN curl http://get.onedata.org/yum/1802/onedata_centos_7x.repo > /etc/yum.repos.d/onedata.repo && \
    yum -y install epel-release && \
    yum -y --enablerepo=onedata install oneclient
RUN cd /opt && \
    tar czf oneclient-1802.tgz oneclient

# Build oneclient 18.02-rc13
FROM centos:7 AS oneclient1802rc13
RUN curl http://get.onedata.org/yum/1802/onedata_centos_7x.repo > /etc/yum.repos.d/onedata.repo && \
    yum -y install epel-release && \
    yum -y --enablerepo=onedata install oneclient-18.02.0.rc13
RUN cd /opt && \
    tar czf oneclient-1802-rc13.tgz oneclient

# Build the main image
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
                   openssh-server \
                   python3 \
                   python3-pip

# Jobs will be run as this user
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
COPY 22-security /etc/condor/config.d/

# Scripts
COPY condor_url_fetch /usr/local/libexec/
COPY job-prepare-hook /usr/local/bin/
COPY get-location /usr/local/bin/
COPY write-resources.py /usr/local/bin/

# OneData versions
RUN rm -rf /opt/oneclient && chown user:user /opt
COPY --from=oneclient2002 /opt/oneclient-2002.tgz /opt/.
COPY --from=oneclient1902 /opt/oneclient-1902.tgz /opt/.
COPY --from=oneclient1802 /opt/oneclient-1802.tgz /opt/.
COPY --from=oneclient1802rc13 /opt/oneclient-1802-rc13.tgz /opt/.
RUN cd /opt && \
    tar xzf oneclient-2002.tgz && mv oneclient oneclient-20.02 && \
    tar xzf oneclient-1902.tgz && mv oneclient oneclient-19.02 && \
    tar xzf oneclient-1802.tgz && mv oneclient oneclient-18.02 && \
    tar xzf oneclient-1802-rc13.tgz && mv oneclient oneclient-18.02-rc13 && \
    rm -f oneclient-*.tgz

# Entrypoint
COPY docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]

RUN pip3 install requests nslookup python-geoip-python3 python-geoip-geolite2 geopy==1.23.0
