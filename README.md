
# Data Processing Challenge

## Introduction 

This project is a technical test, which consists of a script developed in Python
to collect accumulated precipitation data from MERGE/CPTEC and provide daily 
accumulated precipitation data within a specified watershed. The project makes 
this data available via API.

## Project Organization

The src folder contains the source code of the project, which is divided into

- etl.py: contains the scripts responsible for extracting, transforming, and loading the data.
- accumulate_precipitation.py: script that accumulated the precipitation data from MERGE/CPTEC.
- api.py: contains the scripts responsible for the API.

## API Endpoints

#### GET /teste-tecnico/datas-limite
```http://localhost:5000/teste-tecnico/datas-limite?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD```
#### GET /teste-tecnico/media-bacia/obter
```http://localhost:5000/teste-tecnico/media-bacia/obter?start_date=YYYY-MM-DD&watershed_name=XXXX```

 - You can download the Postman collection [here](https://github.com/adaatii/data-processing-challenge/tree/main/postman)

> [!WARNING]
> Replace YYYY-MM-DD with the desired start and end dates. Also, replace XXXX with the desired watershed name.
> The replacement must be done in the URL, in the docker-compose, and docker run commands.

> [!NOTE]
> For the ```/teste-tecnico/datas-limite``` you can use theses dates to test the API: 2024-09-20 and 2024-09-24.

> [!NOTE]
> For the ```/teste-tecnico/media-bacia/obter``` you can use this date and watershed name to test the API: 2024-01-31 and the watershed name: 'xingu'.
> You can check all the available watersheds [here](https://github.com/adaatii/data-processing-challenge/tree/main/contornos).

## Running the project

### Clone the repository

#### Using HTTPS
```bash
git clone https://github.com/adaatii/data-processing-challenge.git
```

#### Using SSH
```bash
git clone git@github.com:adaatii/data-processing-challenge.git
```

### Docker

> [!NOTE]
> To run the project using Docker, you must have Docker installed on your machine.

> [!NOTE]
> The accumulated precipitation script takes some time to run, so it is recommended to wait a while to see the results.

> [!NOTE]
> The containers are running in the background, so to stop them, it is necessary to stop the container `docker stop {container}`
> and remove the container `docker rm {container}` or stop and remove using Docker Desktop.

There are two ways to run the project using Docker:

#### 1. Executing the docker-compose file or building the image and running the container.

#### docker-compose.prec.yml for Accumulated Precipitation

```bash
docker-compose -f docker-compose.prec.yml build
```

```bash
docker-compose -f docker-compose.prec.yml run accumulated_prec --start YYYY-MM-DD --end YYYY-MM-DD
```

#### docker-compose.api.yml for API

```bash
docker-compose -f docker-compose.api.yml build
```

```bash
docker-compose -f docker-compose.api.yml up -d
```

#### 2. Building the image and running the container (Dockerfiles).

#### Accumulated Precipitation

```bash
docker build -t accumulated_precipitation -f Dockerfile.prec .
```

```bash 
docker run -v $(pwd)/output:/app/output accumulated_precipitation --start YYYY-MM-DD --end YYYY-MM-DD
```

#### API

```bash
docker build -t flask_api -f Dockerfile.api .
```

```bash
docker run -d -p 5000:5000 flask_api
```

### Manually

> [!NOTE]
> To run the project manually, you must have Python 3.12 installed on your machine.

### Preparing the environment
Linux:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Windows:
```bash
python3 -m venv .venv
env\Scripts\activate.bat
pip install -r requirements.txt
```

### Running Accumulated Precipitation

```bash
cd src/
python3 accumulated_precipitation.py --start YYYY-MM-DD --end YYYY-MM-DD
```

### Running the API

```bash
cd src/
python3 api.py
```