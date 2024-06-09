import subprocess


def run(command):
    subprocess.run(command, shell=True)


def main():
    # clone ibis-bench
    cmd = "git clone https://lostmygithubaccount/ibis-bench"
    run(cmd)

    # change to ibis-bench directory
    cmd = "cd ibis-bench"
    run(cmd)

    # install ibis-bench
    cmd = "pip install -e ."
    run(cmd)

    # run
    cmd = "just run-all-parquet"
    run(cmd)
