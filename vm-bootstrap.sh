#!/bin/bash

# install brew
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# setup brew
(echo; echo 'eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"') >> $HOME/.bashrc
eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"

# add brew bin to path
#echo 'export PATH="/home/linuxbrew/.linuxbrew/bin:$PATH"' >> $HOME/.bashrc

# install brew packages
brew install python@3.11
brew install just
brew install btop
brew install tree

# aliases
echo 'alias python=python3.11' >> $HOME/.bashrc
echo 'alias pip=pip3.11' >> $HOME/.bashrc
echo 'alias top=btop' >> $HOME/.bashrc
echo 'alias du=du -h -d1' >> $HOME/.bashrc
echo 'alias ..=cd ..' >> $HOME/.bashrc
echo 'alias ...=cd ../..' >> $HOME/.bashrc
echo 'alias ls="ls -1phG -a"' >> $HOME/.bashrc
echo 'alias lsl="ls -l"' >> $HOME/.bashrc

# source
# source $HOME/.bashrc
# 
# # clone ibis-bench
## git clone https://github.com/lostmygithubaccount/ibis-bench
# cd ibis-bench
# 
# # python installs
# pip install -e .
# 
# # run
# just run
