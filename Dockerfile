FROM ubuntu:18.04

WORKDIR /crawl
COPY .  /crawl


ENV TZ=Asia/Shanghai
ENV LANG=C.UTF-8

RUN apt-get update && \
    apt-get install python3-pip -y && \
    ln -s /usr/bin/pip3 /usr/bin/pip



RUN pip install -r requirements.txt && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get install tzdata -y && \
    dpkg-reconfigure --frontend noninteractive tzdata && \
    echo 'export PYTHONUNBUFFERED=1' >> /etc/profile && \
    echo 'export LANG=C.UTF-8' >> /etc/profile && \
    crontab /crawl/crontabfile

CMD  service cron restart && echo `date` >> /var/log/crawl.log && tail -f /var/log/crawl.log
