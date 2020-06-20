#!/bin/bash

sudo yum install -y gcc gcc-c++ pkg-config openssl-devel git
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
cargo build --release
sudo su -c echo "\t*\tsoft\tnofile\t20000" >> "/etc/security/limits.conf"
sudo su -c echo "\t*\thard\tnofile\t20000" >> "/etc/security/limits.conf"
sudo su -c "sed -i 's/4096/unlimited/g' /etc/security/limits.d/20-nproc.conf"

mkdir /tmp/crawler-meta /tmp/crawler-index
export META_DIR=/tmp/crawler-meta INDEX_DIR=/tmp/crawler-index
