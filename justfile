# justfile

# load environment variables
set dotenv-load

# variables
instance_name := "ibis-bench"
instance_type := "n2d-standard-2"
instance_zone := "us-central1-b"

gen_scale_factors := "-s 1 -s 8 -s 16 -s 32 -s 64 -s 128" 

# aliases
alias fmt:=format

# list justfile recipes
default:
    just --list

# build
build:
    just clean-dist
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
    @ruff format .

# publish-test
release-test:
    just build
    @twine upload --repository testpypi dist/* -u __token__ -p ${PYPI_TEST_TOKEN}

# publish
release:
    just build
    @twine upload dist/* -u __token__ -p ${PYPI_TOKEN}

# app
app:
    @streamlit run results.py

# open
open:
    @open https://ibis-bench.streamlit.app

# gen parquet data
gen-data:
    @bench gen-data {{gen_scale_factors}} --csv

# run
run *args:
    @bench run ${args}

# run all parquet queries
run-all-parquet:
    nohup bench2 run | tee out.log &

# run all csv queries
run-all-csv:
    nohup bench2 run --csv | tee out.log &

# run all
run-all:
    just run-all-parquet
    just run-all-csv

# upload tpch data
tpch-upload:
    gsutil -m cp -r tpch_data gs://ibis-bench-tpch

# download tpch CSV data
tpch-download-csv:
    mkdir -p tpch_data/csv
    gsutil -m cp -r gs://ibis-bench-tpch/tpch_data/csv tpch_data

# download tpch Parquet data
tpch-download-parquet:
    mkdir -p tpch_data/parquet
    gsutil -m cp -r gs://ibis-bench-tpch/tpch_data/parquet tpch_data

# uplaod log data
logs-upload:
    gsutil -m cp -r bench_logs_* gs://ibis-bench

# create vm
vm-create:
    gcloud compute instances create {{instance_name}} \
        --zone={{instance_zone}} \
        --machine-type={{instance_type}} \
        --image=ubuntu-2004-focal-v20240519 \
        --image-project=ubuntu-os-cloud \
        --boot-disk-size=1000GB \
        --boot-disk-type=pd-ssd

# ssh into vm
vm-ssh:
    gcloud compute ssh {{instance_name}} --zone={{instance_zone}} --tunnel-through-iap

# suspend vm
vm-suspend:
    gcloud compute instances stop {{instance_name}} --zone={{instance_zone}}

# resume vm
vm-resume:
    gcloud compute instances start {{instance_name}} --zone={{instance_zone}}

# delete vm
vm-delete:
    gcloud compute instances delete {{instance_name}} --zone={{instance_zone}}

# clean dist
clean-dist:
    @rm -rf dist

# clean logs
clean-logs:
    @rm -rf bench_logs_*
    @rm -rf bench_cli_logs
    @rm -f out.log

# clean results
clean-results:
    @rm -rf results_data

# clean app
clean-app:
    @rm -rf app.ddb*
    @rm -rf cache.ddb*

# clean all
clean-all:
    just clean-dist
    just clean-logs
    just clean-app
    just clean-results
