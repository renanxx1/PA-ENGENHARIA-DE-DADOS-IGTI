FROM apache/druid:0.22.1


USER root:root
RUN mkdir -p /druid/
RUN chmod -R 777 /druid/
RUN chown druid /druid


USER druid:druid