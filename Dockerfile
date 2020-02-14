FROM python:3.7-slim
ENV PYTHONUNBUFFERED=1
ADD /crawler /
RUN pip install -r requirements.txt
CMD [ "python", "./crawler/execute.py" ]


