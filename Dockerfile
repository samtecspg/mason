FROM ubuntu:20.04

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -qq -y gcc g++ zlibc zlib1g-dev libssl-dev libbz2-dev libsqlite3-dev libncurses5-dev libgdbm-dev libgdbm-compat-dev liblzma-dev libreadline-dev uuid-dev libffi-dev tk-dev wget curl git make sudo bash-completion tree vim software-properties-common direnv python3-pip
RUN curl https://pyenv.run | bash
ENV PATH="/root/.pyenv/bin:${PATH}"
RUN pyenv install 3.8.0

COPY ./requirements.txt /app/requirements.txt

WORKDIR /app
COPY . /app
RUN mkdir /mason
RUN mkdir /root/.aws/
RUN touch /root/.aws/credentials

ENV MASON_HOME /mason/
RUN cp .env.example ${MASON_HOME}/.env
RUN rm .env 

ENV KUBECONFIG /app/.kube/config

# install java for dask-sql
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-11-jre
ENV JAVA_HOME /usr/bin/java

# TODO Remove this and replace all instances with k8s python client
RUN ./scripts/install_kubectl.sh

RUN ./scripts/test.sh
RUN ./scripts/install.sh

# Remove if you do not wish to install the example configuration or operators
RUN mason apply mason/examples/

CMD ["./scripts/run.sh"]
