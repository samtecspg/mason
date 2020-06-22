FROM python:3.7

COPY ./requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip3 install -r requirements.txt

COPY . /app

RUN chmod +x /app/demos/run_demo.sh
RUN pip3 install mypy

RUN mkdir /mason

ENV MASON_HOME /mason/
RUN cp .env.example ${MASON_HOME}/.env
RUN rm .env 

ENV KUBECONFIG /app/.kube/config

RUN ./scripts/test.sh
RUN ./scripts/install.sh

# Remove if you do not wish to install the example configuration or operators
RUN mason config mason/examples/configs/
RUN mason register mason/examples/

RUN ./scripts/install_kubectl.sh

CMD ["./scripts/run.sh"]
