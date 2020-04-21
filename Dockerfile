FROM python:3.7

COPY ./requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip3 install -r requirements.txt

COPY . /app

RUN chmod +x /app/demos/run_demo.sh

RUN pip3 install mypy
RUN ./scripts/test.sh
RUN ./scripts/install.sh

RUN mkdir /mason
ENV MASON_HOME /mason/
ENV KUBECONFIG /app/.kube/config

# Remove if you do not wish to install the example configuration or operators
RUN mason config examples/configs/
RUN mason register examples/operators/table/
RUN mason register examples/operators/job/

RUN ./scripts/install_kubectl.sh

CMD ["./scripts/run.sh"]
