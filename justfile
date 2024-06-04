# Justfile

# load environment variables
set dotenv-load

# variables
#extras := "-s 1 -s 10 -n"
extras := "-s 1 -s 10 -s 20 -s 40 -s 50 -s 100 -s 150 -s 200"
#extras := "-s 20 -s 40 -n 1 -n 64 -n 128 --cloud-logging"

instance_type := "c3-highcpu-22"

# aliases
alias fmt:=format

# list justfile recipes
default:
    just --list

# build
build:
    just clean
    @python -m build

# setup
setup:
    @uv pip install -r dev-requirements.txt
    just install

# install
install:
    @uv pip install -e . --reinstall-package ibis_bench

# format
format:
    @ruff format . || True

# publish-test
release-test:
    just build
    @twine upload --repository testpypi dist/* -u __token__ -p ${PYPI_TEST_TOKEN}

# publish
release:
    just build
    @twine upload dist/* -u __token__ -p ${PYPI_TOKEN}

# clean
clean:
    @rm -r dist || True

# app
app:
    @streamlit run app.py

# open
open:
    @open https://ibis-bench.streamlit.app

# gen data
gen-data:
    @bench gen-data {{extras}}

# run
run-dataframe:
    @bench run ibis-duckdb          {{extras}} -c -i {{instance_type}}
    @bench run ibis-datafusion      {{extras}} -c -i {{instance_type}}
    @bench run polars-lazy          {{extras}} -c -i {{instance_type}}
    @bench run ibis-polars          {{extras}} -c -i {{instance_type}}

run-sql:
    @bench run ibis-duckdb-sql      {{extras}} -c -i {{instance_type}}
    @bench run ibis-datafusion-sql  {{extras}} -c -i {{instance_type}}

run:
    just run-dataframe
    just run-sql

# e2e
e2e:
    just gen-data
    just run

# cloud shenanigans
vm-create:
    gcloud compute instances create ibis-bench \
        --zone=us-central1-b \
        --machine-type={{instance_type}} \
        --image=ubuntu-2004-focal-v20240519 \
        --image-project=ubuntu-os-cloud \
        --boot-disk-size=500GB \
        --boot-disk-type=pd-ssd

vm-ssh:
    gcloud compute ssh ibis-bench --zone=us-central1-b --tunnel-through-iap

vm-suspend:
    gcloud compute instances stop ibis-bench --zone=us-central1-b

vm-resume:
    gcloud compute instances start ibis-bench --zone=us-central1-b

vm-delete:
    gcloud compute instances delete ibis-bench --zone=us-central1-b
