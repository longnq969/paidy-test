# paidy-test


> Step to reproduce:

```
# clone this repository
git clone https://github.com/longnq969/paidy-test.git

# cd into it
cd paidy-test

# run docker-compose
docker-compose up --build

# ssh into prefect_agent
docker exec -it prefect_agent sh

# run init deployment and the flow is up and scheduled as set
python ./scripts/init_deployment.py --interval 86400 --timezone "Asia/Ho_Chi_Minh" --source-folder "./data/sources"

```
