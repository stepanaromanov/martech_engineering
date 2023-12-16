#download images
docker pull postgres:latest
docker pull apache/superset:latest

#create volumes in docker if something happens to database
docker volume create postgres_volume_main

#create a network
docker network create main_net

#setup superset
docker run -d \
    --net=main_net \
    -p 8080:8088 \
    -e "SUPERSET_SECRET_KEY=XXXXXXXXXX" \
    --name superset_main apache/superset

#superset changes
docker exec -it superset_main superset fab create-admin \
              --username XXXXXXXXXX \
              --firstname XXXXXXXXXX \
              --lastname XXXXXXXXXX \
              --email XXXXXXXXXX \
              --password XXXXXXXXXX

#Migrate local DB to latest
docker exec -it superset_main superset db upgrade

#Setup roles
docker exec -it superset_main superset init

#setup postgres
docker run -d \
     --name postgres_main \
     -e POSTGRES_PASSWORD=XXXXXXXXXX \
     -e POSTGRES_USER=XXXXXXXXXX \
     -e POSTGRES_DB=XXXXXXXXXX \
     -v postgres_volume_main://var/lib/postgresql/data \
     --net=main_net \
     -p 5432:5432 \
     postgres


# Optionally: change talisman enabled configuration to avoid login problem
docker exec -it superset_main /bin/bash
echo "TALISMAN_ENABLED = False" >> superset/config.py
# Check changes:
cat superset/config.py
exit
docker restart superset_main
