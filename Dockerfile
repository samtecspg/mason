FROM python:3.7

COPY ./requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip3 install -r requirements.txt

COPY . /app

RUN pip3 install mypy
RUN ./scripts/install.sh

RUN mkdir /mason
ENV MASON_HOME /mason/

# Remove if you do not wish to install the example configuration or operators
RUN mason config examples/configs/
RUN mason register examples/operators/table/

ENTRYPOINT [ "mason" ]

CMD [ "run" ]
