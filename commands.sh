#create volumes in docker if something happens to database
docker volume create ***POSTGRES_VOLUME_NAME***

#create a network
docker network create ***NETWORK_NAME***

#setup superset
docker run -d --net=***NETWORK_NAME*** -p 8080:8088 -e "SUPERSET_SECRET_KEY=***YOUR_SECRET_KEY***" --name ***SUPERSET*** apache/superset

#superset changes
docker exec -it superset superset fab create-admin \
              --username admin \
              --firstname ***NAME*** \
              --lastname ***LASTNAME*** \
              --email ***EMAIL*** \
              --password ***SUPERSET_PASSWORD***

#Migrate local DB to latest
docker exec -it superset superset db upgrade

#Setup roles
docker exec -it superset superset init

#setup postgres
docker run -d \
     --name ***POSTGRES_CONTAINER_NAME*** \
     -e POSTGRES_PASSWORD=***POSTGRES_PASSWORD*** \
     -e POSTGRES_USER=***POSTGRES_USER*** \
     -e POSTGRES_DB=***POSTRES_DB_NAME*** \
     -v ***POSTGRES_VOLUME_NAME***://var/lib/postgresql/data \
     --net=***NETWORK_NAME*** \
     -p 5432:5432 \
     postgres

#Remove
docker stop ***POSTGRES_CONTAINER_NAME***
docker stop ***SUPERSET***
docker volume prune