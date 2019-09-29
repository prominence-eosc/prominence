FROM centos:7

# Dependencies
RUN yum -y install wget

# Install yum repository
RUN cd /etc/yum.repos.d && \
    wget http://research.cs.wisc.edu/htcondor/yum/repo.d/htcondor-development-rhel7.repo

# Import signing key
RUN wget http://research.cs.wisc.edu/htcondor/yum/RPM-GPG-KEY-HTCondor && \
    rpm --import RPM-GPG-KEY-HTCondor && \
    rm RPM-GPG-KEY-HTCondor

# Install HTCondor
RUN yum -y install condor

ENTRYPOINT ["/usr/sbin/condor_master",  "-f"]
