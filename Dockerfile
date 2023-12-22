FROM ubuntu:22.04

RUN apt-get -y install python3 python3-pip
RUN python3 -m pip install beautifulsoup4 flask requests asyncio aiohttp psycopg2

ENTRYPOINT ["python", "mapreduce.py"]




