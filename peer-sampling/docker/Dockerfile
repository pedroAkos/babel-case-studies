FROM openjdk:8-slim
LABEL maintainer="Pedro Ákos Costa <pah.costa@campus.fct.unl.pt>"

COPY pkg/install-packages.sh .
RUN ./install-packages.sh

WORKDIR code
ADD network/start.sh ./start.sh
ADD network/peer-sampling.jar ./peer-sampling.jar
ADD network/config.properties ./config.properties
ADD network/log4j2.xml .

RUN mkdir config

#ADD config/inet100Latencies_x0.005.txt config/latencyMap.txt
ADD config/inet100Latencies_x0.04.txt config/latencyMap.txt
#ADD config/ips100.txt config/ips.txt
ADD network/setupTc.sh .
ADD config/ips100.txt config/ips.txt

ENTRYPOINT ["./setupTc.sh"]
