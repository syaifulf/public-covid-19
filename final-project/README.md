# AIRFLOW DOCKER
Final Project DE
This final project is about Dockerize ETL Pipeline using ETL tools Airflow that extract Public API data from PIKOBAR, then load into MySQL (Staging Area) and finally aggregate the data and save into PostgreSQL.

## Install

- Clone this git repo into your local repository
```
$ git clone https://github.com/syaifulf/public-covid-19.git
```
- Rename .env.example to be .env and customize the data information according to your needs
- Install Docker in https://docs.docker.com/engine/install/
- Install requirement library
  ```
  pip install  pip install -r /path/to/requirements.txt
  ```
- Compose file docker-compose to build containers
  ```
  docker-compose up -d
  ```

To be continued...

