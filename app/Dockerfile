FROM python:3.7-alpine
WORKDIR /app
COPY . /app
RUN pip install --trusted-host pypi.python.org -r requirements.txt
EXPOSE 26500
ENV NAME World
CMD ["python", "main.py"]

