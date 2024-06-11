#!/bin/bash

# install brew
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# setup brew
(echo; echo 'eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"') >> $HOME/.bashrc
eval "$(/home/linuxbrew/.linuxbrew/bin/brew shellenv)"

# install brew packages
brew install python@3.11
brew install just
brew install btop
brew install tree

# aliases
echo 'alias python="python3.11"' >> $HOME/.bashrc
echo 'alias pip="pip3.11"' >> $HOME/.bashrc
echo 'alias top="btop"' >> $HOME/.bashrc
echo 'alias du="du -h -d1"' >> $HOME/.bashrc
echo 'alias v="vi"' >> $HOME/.bashrc
echo 'alias ..="cd .."' >> $HOME/.bashrc
echo 'alias ...="cd ../.."' >> $HOME/.bashrc
echo 'alias ls="ls -1phG -a"' >> $HOME/.bashrc
echo 'alias lsl="ls -l"' >> $HOME/.bashrc
