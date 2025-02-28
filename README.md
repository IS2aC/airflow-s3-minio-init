
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' airflow-s-3_1625ea-scheduler-1
172.29.0.4