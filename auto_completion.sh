#!/usr/bin/env bash
# Inspired by: https://qone.io/python/bash/zsh/2016/04/24/python-click-auto-complete-bash-zsh.html

if [[ $(basename $SHELL) = 'bash' ]];
then
    if [[ -f ~/.bashrc ]];
    then
        echo "Installing bash autocompletion..."
        grep -q 'esque-autocompletion' ~/.bashrc
        if [[ $? -ne 0 ]]; then
            echo "" >> ~/.bashrc
            echo 'eval "$(_ESQUE_COMPLETE=source esque)"' >> ~/.esque-autocompletion.sh
            echo "source ~/.esque-autocompletion.sh" >> ~/.bashrc
        fi
    fi
elif [[ $(basename $SHELL) = 'zsh' ]];
then
    if [[ -f ~/.zshrc ]];
    then
        echo "Installing zsh autocompletion..."
        grep -q 'esque-autocompletion' ~/.zshrc
        if [[ $? -ne 0 ]]; then
            echo "" >> ~/.zshrc
            echo 'eval "$(_ESQUE_COMPLETE=source_zsh esque)"' >> ~/.esque-autocompletion.zsh
            echo "source ~/.esque-autocompletion.zsh" >> ~/.zshrc
        fi
    fi
fi