#!/bin/bash

git clone https://github.com/agajews/hyper-tls.git ../hyper-tls
git clone https://github.com/agajews/hyper.git ../hyper
git clone https://github.com/agajews/hyper-rustls.git ../hyper-rustls
git clone https://github.com/agajews/reqwest.git ../reqwest
sudo yum install -y gcc gcc-c++ pkg-config openssl-devel git
sudo yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
sudo yum install -y htop nload perf tmux
sudo yum install -y nfs-utils NetworkManager
# sudo mkdir -p /mnt/web-crawler-fs
# sudo mount 10.0.0.5:/web-crawler-fs /mnt/web-crawler-fs
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env
rustup default nightly
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
sudo su -c "echo 'net.ipv4.ip_local_port_range = 1024 65535' >> /etc/sysctl.conf"
sudo sysctl -p
# sudo ifconfig eno1 txqueuelen 10000
# sudo ethtool -G eno1 rx 2047
# sudo ethtool -G eno1 rx-jumbo 8191
# sudo ethtool -G eno1 tx 2047
# sudo ethtool -L eno1 combined 74
# sudo ethtool -A eno1 autoneg off rx off tx off
# sudo ethtool -C eno1 rx-usecs 0

sudo yum install -y dnsmasq bind-utils
sudo groupadd -r dnsmasq
sudo useradd -r -g dnsmasq dnsmasq
cat << EOF > dnsmasq.conf
# Server Configuration
listen-address=127.0.0.1
port=53
bind-interfaces
user=dnsmasq
group=dnsmasq
pid-file=/var/run/dnsmasq.pid
# Name resolution options
resolv-file=/etc/resolv.dnsmasq
cache-size=10000
neg-ttl=60
domain-needed
bogus-priv
EOF

sudo cp dnsmasq.conf /etc

sudo su -c 'echo "nameserver 169.254.169.254" > /etc/resolv.dnsmasq'
sudo systemctl restart dnsmasq.service
sudo systemctl enable  dnsmasq.service

dig aws.amazon.com

dig aws.amazon.com @127.0.0.1 && sudo su -c 'echo "supersede domain-name-servers 127.0.0.1;" >> /etc/dhcp/dhclient.conf' && sudo dhclient

wget https://docs.cloud.oracle.com/en-us/iaas/Content/Resources/Assets/secondary_vnic_all_configure.sh
chmod +x secondary_vnic_all_configure.sh
mv secondary_vnic_all_configure.sh ..

sudo firewall-cmd --zone=public --permanent --add-port=8000/tcp
sudo firewall-cmd --reload
