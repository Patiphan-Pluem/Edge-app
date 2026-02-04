FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1

RUN pip install synchrophasor psycopg2-binary psutil

WORKDIR /app

COPY TinyPMU.py TinyPDC.py ems_simulator.py /app/

CMD ["python3", "TinyPMU.py"]