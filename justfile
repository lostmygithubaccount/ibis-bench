# Justfile

# load environment variables
set dotenv-load

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
