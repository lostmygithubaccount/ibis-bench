# Justfile

# load environment variables
set dotenv-load

# variables
#extras := "-s 1 -s 10 -n"
extras := "-s 1 -s 10 -s 20 -s 40 -s 50 -s 100 -s 150"
#extras := "-s 20 -s 40 -n 1 -n 64 -n 128 --cloud-logging"

instance_type := "c3-highcpu-22"
#instance_type := "c3d-highmem-16"

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
    @pip install -e . --reinstall-package ibis_bench

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

# open
open:
    @open https://ibis-bench.streamlit.app

# gen data
gen-data:
    @bench gen-data {{extras}} -c

# run
run *args:
    @bench run {{args}} -i {{instance_type}}

run-dataframe:
    just run ibis-duckdb {{extras}}
    just run ibis-datafusion  {{extras}}
    just run polars-lazy  {{extras}}
    just run ibis-polars  {{extras}}

run-sql:
    just run ibis-duckdb-sql  {{extras}}
    just run ibis-datafusion-sql  {{extras}}

run-all:
    just run-dataframe
    just run-sql

# e2e
e2e:
    just data-download
    just run

# cloud shenanigans
data-upload:
    gsutil -m cp -r tpch_data gs://ibis-bench-tpch

data-download:
    mkdir -p tpch_data
    gsutil -m cp -r gs://ibis-bench-tpch/tpch_data .

logs-upload:
    gsutil -m cp -r bench_logs_* gs://ibis-bench
    gsutil -m cp -r bench_cli_logs gs://ibis-bench

vm-create:
    gcloud compute instances create ibis-bench \
        --zone=us-central1-b \
        --machine-type={{instance_type}} \
        --image=ubuntu-2004-focal-v20240519 \
        --image-project=ubuntu-os-cloud \
        --boot-disk-size=1000GB \
        --boot-disk-type=pd-ssd

vm-ssh:
    gcloud compute ssh ibis-bench --zone=us-central1-b --tunnel-through-iap

vm-suspend:
    gcloud compute instances stop ibis-bench --zone=us-central1-b

vm-resume:
    gcloud compute instances start ibis-bench --zone=us-central1-b

vm-delete:
    gcloud compute instances delete ibis-bench --zone=us-central1-b

vm-run:
    gcloud compute ssh ibis-bench --zone=us-central1-b --tunnel-through-iap -- bash -s < vm-bootstrap.sh

# gcloud compute ssh ibis-bench --zone=us-central1-b --tunnel-through-iap --command "just e2e &"
