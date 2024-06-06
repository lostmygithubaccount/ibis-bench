# Justfile

# load environment variables
set dotenv-load

# variables
all_systems := "ibis-duckdb ibis-duckdb-sql ibis-datafusion ibis-datafusion-sql polars-lazy ibis-polars"

#extras := "-s 1"
#extras := "-s 1 -s 10 -s 20 -s 40 -s 50 -s 100 -s 150 -s 200"
extras := "-s 1 -s 8 -s 16 -s 32 -s 64 -s 128" 

# instance_type := "work laptop"
# instance_type := "personal laptop"
instance_type := "c3-highcpu-22"
# instance_type := "c3d-highmem-16"
# instance_type := "c3-highmem-88"

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
    @pip install -r dev-requirements.txt
    just install

# install
install:
    @pip install -e .

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

# clean logs
clean-logs:
    @rm -r bench_logs_* || True
    @rm -r bench_cli_logs || True

# clean results
clean-results:
    @rm -r results_data || True

# clean all
clean-all:
    just clean
    just clean-logs
    just clean-results

# app
app:
    @streamlit run app.py

# dev app
app-dev:
    @streamlit run dev-app.py

# open
open:
    @open https://ibis-bench.streamlit.app

# gen data
gen-data:
    @bench gen-data {{extras}} -c

# run
run *args:
    @bench run {{args}} -i "{{instance_type}}"

# run all parquet queries
run-all-parquet:
    just run {{all_systems}} {{extras}}

# run all csv queries
run-all-csv:
    just run {{all_systems}} {{extras}} --csv

# run all queries
run-all:
    just run-all-parquet
    just run-all-csv

# cache json to parquet
cache:
    @bench cache-json

# e2e
e2e:
    just tpch-download
    just run-all
    just cache
    just logs-upload

# upload tpch data
tpch-upload:
    gsutil -m cp -r tpch_data gs://ibis-bench-tpch

# download tpch data
tpch-download:
    mkdir -p tpch_data
    gsutil -m cp -r gs://ibis-bench-tpch/tpch_data .

# uplaod log data
logs-upload:
    gsutil -m cp -r bench_logs_* gs://ibis-bench

# create vm
vm-create:
    gcloud compute instances create ibis-bench \
        --zone=us-central1-b \
        --machine-type={{instance_type}} \
        --image=ubuntu-2004-focal-v20240519 \
        --image-project=ubuntu-os-cloud \
        --boot-disk-size=1000GB \
        --boot-disk-type=pd-ssd

# ssh into vm
vm-ssh:
    gcloud compute ssh ibis-bench --zone=us-central1-b --tunnel-through-iap

# suspend vm
vm-suspend:
    gcloud compute instances stop ibis-bench --zone=us-central1-b

# resume vm
vm-resume:
    gcloud compute instances start ibis-bench --zone=us-central1-b

# delete vm
vm-delete:
    gcloud compute instances delete ibis-bench --zone=us-central1-b

# run on VM
#vm-run:
#    gcloud compute ssh ibis-bench --zone=us-central1-b --tunnel-through-iap -- bash -s < vm-bootstrap.sh

# gcloud compute ssh ibis-bench --zone=us-central1-b --tunnel-through-iap --command "just e2e &"
