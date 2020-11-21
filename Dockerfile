FROM python:3.6.12-slim-buster

ENV TZ=Asia/Shanghai LANG=C.UTF-8

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "./main.py" ]