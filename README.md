# ex_3
The script does the following:
1. Download EGRUL zip file
2. Selects companies by a specific OKVED code and store it in _telecom_companies_ table
3. Parse hh.ru site - store vacanties by filter in _vacancies_ table
4. Select jobs with companies with a specific OKVED code and store data in _ex3_result_ table

Education project

### How to run
```
docker compose up -d
```
and run _ex_3_etl_ DAG in web interface or test it in terminal
```
airflow dags test ex_3_etl
```
make sure what the _logs_, _data_, _dags_ folders are writable for Airflow. Or set AIRFLOW_UID to current user
```
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```

web server running on non-standard external port http://localhost:8882, postgres: localhost:5444

Dataset is stored in the default database (_id=postgres_default_), you may specify connection in .env file 
```
AIRFLOW_CONN_POSTGRES_DEFAULT
```

![screen 1](https://github.com/Arsenicu/ex_3/blob/main/sc1.png)

![screen 1](https://github.com/Arsenicu/ex_3/blob/main/sc4.png)
