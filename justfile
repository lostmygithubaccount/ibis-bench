# Justfile

# load environment variables
set dotenv-load

# variables
extras := "-s 1 -s 10 -s 20 -s 40 -s 50 -s 100 -s 150 -h 1 -h 64 -h 128"

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

# gen data
gen-data:
    @bench gen-data ${extras}

# run
run:
    @bench run ibis-duckdb     ${extras}
    @bench run ibis-datafusion ${extras}
    @bench run ibis-polars     ${extras}
    @bench run polars-lazy     ${extras}

# e2e
e2e:
    just gen-data
    just run
