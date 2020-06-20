#!/bin/bash

sudo yum install -y gcc gcc-c++ pkg-config openssl-devel git
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
