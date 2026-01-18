FROM python:3.9-alpine
WORKDIR /app
COPY events.py requirements.txt ./
RUN pip install -r requirements.txt
EXPOSE 8082
CMD ["python", "events.py"]