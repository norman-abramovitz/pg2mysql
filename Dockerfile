FROM debian:buster

ADD mysql.key .
ADD entrypoint.sh .

VOLUME [ "/src" ]

RUN apt-get update && \
    apt-get install -y curl gnupg && \
    echo 'deb http://apt.postgresql.org/pub/repos/apt/ buster-pgdg main' >> /etc/apt/sources.list.d/postgresql.list && \
    curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    apt-key add mysql.key && \
    echo 'deb http://repo.mysql.com/apt/debian/ buster mysql-5.7' >> /etc/apt/sources.list.d/mysql.list && \
    echo "mysql-community-server/root-pass: password tiger" | debconf-set-selections && \
    echo "mysql-community-server/re-root-pass: password tiger" | debconf-set-selections && \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y postgresql-9.6 mysql-server build-essential && \
    curl -O https://dl.google.com/go/go1.13.6.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go1.13.6.linux-amd64.tar.gz && \
    rm go1.13.6.linux-amd64.tar.gz && \
    ln -s /usr/local/go/bin/go /usr/local/bin/go

WORKDIR /src

ENTRYPOINT [ "/entrypoint.sh" ]