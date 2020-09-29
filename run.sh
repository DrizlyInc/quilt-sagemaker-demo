#!/bin/bash
echo ENV:$ENV
if [[ "$1" = train ]]
then
    cd app
    python eta/train.py
    echo trained
elif [[ "$ENV" = "local" ]]
then
    export ETAS_ENV="local"
    echo Local Development
    pwd
    #python /app/eta/local_development/populate_redis.py
    echo populated redis
    # Get file from s3 or from local
    echo Copying model to local dir
    mkdir -p /opt/ml/model/
    cp app/eta/data/composed_model.pkl /opt/ml/model/
    cp app/eta/data/health-check-data.csv /opt/ml/model/
    echo Running flask
    python -m flask run --host=0.0.0.0 --port=8080
else
# TODO: Get health check file from s3
# TODO: Get correct model dir from s3
    export ETAS_ENV="production"
    #python -c "import t4; t4.Package.install('aleksey/fashion-mnist-clf', registry='s3://alpha-quilt-storage', dest='.')"
    #cp aleksey/fashion-mnist-clf/clf.h5 clf.h5
    python -m flask run --host=0.0.0.0 --port=8080
fi
