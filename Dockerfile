FROM python:3.7-slim
ENV PYTHONUNBUFFERED=1
ADD reddit-scraper.py /
ADD requirements.txt /
ADD cogniflare-rd-1298fa5958c1.json /
RUN pip install -r requirements.txt
CMD [ "python", "./reddit-scraper.py" ]


