import os
import subprocess

instance_type = "work laptop"
all_systems = [
    "ibis-duckdb",
    "ibis-duckdb-sql",
    "ibis-datafusion",
    "ibis-datafusion-sql",
    "polars-lazy",
    "ibis-polars",
]
all_sfs = [
    1,
    8,
    16,
    32,
    64,
    128,
]
all_qs = range(1, 23)


def main():
    for system in all_systems:
        for sf in all_sfs:
            for q in all_qs:
                cmd = f"bench run {system} -s {sf} -q {q} -i '{instance_type}'"

                print(f"running: {cmd}")
                res = subprocess.run(cmd, shell=True)
                if res.returncode != 0:
                    print(f"failed to run: {cmd}")

                print(f"finished running: {cmd}")


if __name__ == "__main__":
    main()
