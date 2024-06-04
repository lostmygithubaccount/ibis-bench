# Justfile

# load environment variables
set dotenv-load

# variables
#extras := "-s 1 -s 10 -n"
extras := "-s 1 -s 10 -s 20 -s 40 -s 50 -s 100 -s 150 -n 1 -n 64 -n 128 --cloud-logging"
#extras := "-s 20 -s 40 -n 1 -n 64 -n 128 --cloud-logging"

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
    @bench run ibis-duckdb     {{extras}}
    @bench run ibis-datafusion {{extras}}
    @bench run polars-lazy     {{extras}}
    @bench run ibis-polars     {{extras}}

run-sql:
    @bench run ibis-duckdb-sql {{extras}}
    @bench run ibis-datafusion-sql {{extras}}

run:
    just run-dataframe
    just run-sql

# e2e
e2e:
    just gen-data
    just run

# cloud shenanigans
create-vm:
    gcloud compute instances create ibis-bench \
        --zone=us-central1-b \
        --machine-type=c3-highcpu-22 \
        --image=ubuntu-2004-focal-v20240519 \
        --image-project=ubuntu-os-cloud

#--image-family=ubuntu-2004-lts
ssh-vm:
    gcloud compute ssh ibis-bench --zone=us-central1-b --tunnel-through-iap
