This layer should handle persisting past execution and retrieving data for future use

## Setup
For now, it uses a local version of postgres, postgres is started in a docker container via:
```bash
docker run --name arbo-db \
  -e POSTGRES_USER=arbo_user \
  -e POSTGRES_PASSWORD=arbo_pass \
  -e POSTGRES_DB=arbo_state \
  -p 5433:5432 \
  -d postgres
```
**Note**: might need to change port mapping

It is also possible to connect pgAdmin4 to this database for easier access

# TODO maybe explain structure of code here