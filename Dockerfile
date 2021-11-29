FROM python:3.8-slim

ARG UID
ARG GID


RUN pip install --upgrade pip
RUN mkdir /app
WORKDIR "/app"
COPY ./requirements.txt /app/requirements.txt
COPY ./requirements_dev.txt /app/requirements_dev.txt
RUN pip install -r /app/requirements.txt
RUN pip install -r /app/requirements_dev.txt
ENV PYTHONPATH=/app/src

RUN addgroup --gid $GID user
RUN useradd --no-log-init --comment "Default user" --create-home --uid $UID --gid $GID user
RUN passwd -d user
RUN chown -R user /app
USER user