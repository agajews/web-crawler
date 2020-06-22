#!/bin/bash

sudo yum install -y gcc gcc-c++ pkg-config openssl-devel git
sudo yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
sudo yum install -y htop nload perf
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env
cargo build --release
cargo install flamegraph
sudo su -c 'echo "	*	soft	nofile	1000000" >> /etc/security/limits.conf'
sudo su -c 'echo "	*	hard	nofile	1000000" >> /etc/security/limits.conf'
sudo su -c "sed -i 's/4096/unlimited/g' /etc/security/limits.d/20-nproc.conf"

sudo su -c "echo 'net.core.wmem_max=12582912' >> /etc/sysctl.conf"
sudo su -c "echo 'net.core.rmem_max=12582912' >> /etc/sysctl.conf"
sudo su -c "echo 'net.ipv4.tcp_rmem= 10240 87380 12582912' >> /etc/sysctl.conf"
sudo su -c "echo 'net.ipv4.tcp_wmem= 10240 87380 12582912' >> /etc/sysctl.conf"
sudo su -c "echo 'net.ipv4.tcp_window_scaling = 1' >> /etc/sysctl.conf"
sudo su -c "echo 'net.ipv4.tcp_timestamps = 1' >> /etc/sysctl.conf"
sudo su -c "echo 'net.ipv4.tcp_sack = 1' >> /etc/sysctl.conf"
sudo su -c "echo 'net.ipv4.tcp_no_metrics_save = 1' >> /etc/sysctl.conf"
sudo su -c "echo 'net.core.netdev_max_backlog = 20000' >> /etc/sysctl.conf"
sudo sysctl -p
sudo ifconfig eth0 txqueuelen 10000
sudo ethtool -G eth0 rx 16384
sudo su -c "curl https://raw.githubusercontent.com/awslabs/aws-support-tools/master/EC2/AutomateDnsmasq/AutomateDnsmasq.sh | sed 's/cache-size=500/cache-size=50000\nmin-cache-ttl=3600/g' | sed 's/127.0.0.1, \${NAMESERVER}/127.0.0.1/g' | sh"

mkdir /tmp/crawler-meta /tmp/crawler-index
export META_DIR=/tmp/crawler-meta INDEX_DIR=/tmp/crawler-index
