# ibis-bench

***Benchmarking with Ibis.***

## Overview

Ibis is an ***interface***, not an ***engine***. There's no such thing as a zero-cost abstraction with no overhead, but Ibis is designed to add as little as possible to your workloads. This repository is used to:

- benchmark various engines via the same interface
- roughly assess the overhead of using Ibis (by comparing with native queries)

with a focus on the former and, currently, single-node OLAP engines.

## Getting started

Install the package and CLI.

### Developer setup

Install [`gh`](https://github.com/cli/cli) and [`just`](https://github.com/casey/just), then:

```
gh repo clone lostmygithubaccount/ibis-bench
cd ibis-bench
just setup
. .venv/bin/activate
```

### Pip install

Run:

```bash
pip install --upgrade ibis-bench
```

## Usage

Use the CLI to run benchmarks.

```bash
bench
```
