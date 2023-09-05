FROM flink:1.13

ENV SEATUNNEL_VERSION="2.3.2"
ENV SEATUNNEL_HOME="/opt/seatunnel"

RUN mkdir -p $SEATUNNEL_HOME

WORKDIR "/opt/seatunnel"

ADD apache-seatunnel-incubating-${SEATUNNEL_VERSION}-bin.tar.gz /opt/
RUN mv /opt/apache-seatunnel-incubating-${SEATUNNEL_VERSION}/* .
RUN rm -rf /opt/apache-seatunnel-incubating-${SEATUNNEL_VERSION}*
RUN cp /etc/apt/sources.list /etc/apt/sources.list.bak && sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list
RUN apt-get update
RUN apt-get install -y vim procps