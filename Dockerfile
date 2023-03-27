FROM amazon/aws-cli 

FROM python:3.8

WORKDIR /home/wiwi

RUN sudo mkdir /home/wiwi; exit 0 

RUN sudo mkdir /home/wiwi/data; exit 0

RUN sudo mkdir /home/wiwi/csv; exit 0

RUN apt-get update && apt install -y p7zip-full

COPY . /home/wiwi/

RUN --mount=type=cache,target=/root/.cache pip install -r requirements.txt

CMD [ "python", "./main.py" ]