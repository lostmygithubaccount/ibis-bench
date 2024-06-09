import subprocess


def run(command):
    subprocess.run(command, shell=True)


def main():
    # install brew
    cmd = '/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"'
    run(cmd)

    # setup brew
    cmd = "echo 'eval \"$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)\"' >> $HOME/.bashrc"
    run(cmd)
    cmd = 'eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"'
    run(cmd)

    # install brew packages
    brew_packages = ["python@3.11", "just", "btop", "tree"]
    for package in brew_packages:
        cmd = f"brew install {package}"
        run(cmd)

    # add aliases
    aliases = [
        ("python", "python3.11"),
        ("pip", "pip3.11"),
        ("top", "btop"),
        ("du", "du -h -d1"),
        ("v", "vi"),
        ("..", "cd .."),
        ("...", "cd ../.."),
        ("ls", "ls -1phG -a"),
        ("lsl", "ls -l"),
    ]
    for alias in aliases:
        cmd = f"echo 'alias {alias[0]}=\"{alias[1]}\"' >> $HOME/.bashrc"
        run(cmd)

    # source bashrc
    cmd = "source $HOME/.bashrc"
    run(cmd)

    # clone ibis-bench
    cmd = "git clone https://lostmygithubaccount/ibis-bench"
    run(cmd)

    # change to ibis-bench directory
    cmd = "cd ibis-bench"
    run(cmd)

    # install ibis-bench
    cmd = "pip install -e ."
    run(cmd)
