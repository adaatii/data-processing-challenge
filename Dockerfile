FROM python:3

# Definir o diretório de trabalho dentro do contêiner
WORKDIR /app

COPY . .

RUN mkdir -p ./output

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "daily_accumulations.py"]
