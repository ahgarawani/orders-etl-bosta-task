####################################################################################################################
# Setup containers to run Airflow

docker-spin-up:
	docker compose up airflow-init && docker compose up --build -d

perms:
	sudo mkdir -p logs plugins config dags && sudo chmod -R u=rwx,g=rwx,o=rwx logs plugins config dags

setup-conn:
	docker exec scheduler python /opt/airflow/setup_connections.py

do-sleep:
	sleep 30

ask-email:
	@read -p "Enter email for failure alerts (or press Enter to skip): " email; \
	if [ -n "$$email" ]; then \
		echo "ALERTS_EMAIL=$$email" > .env; \
	fi

up: ask-email perms docker-spin-up do-sleep setup-conn

down:
	docker compose down --volumes --rmi all
	rm -f .env

restart: down up

sh:
	docker exec -ti webserver bash


