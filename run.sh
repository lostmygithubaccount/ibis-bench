#!/bin/bash

bench run ibis-duckdb -S 8 -S 16 -S 32 -S 50 -S 100
bench run ibis-datafusion -S 8 -S 16 -S 32 -S 50 -S 100
bench run ibis-polars -S 8 -S 16 -S 32 -S 50 -S 100
bench run polars-lazy -S 8 -S 16 -S 32 -S 50 -S 100
