#!/bin/bash

VOLUME=$(oci bv volume create --display-name web-crawler-block-3 --size-in-gbs 500 --vpus-per-gb 20 --availability-domain zrEY:US-ASHBURN-AD-1 --compartment-id ocid1.tenancy.oc1..aaaaaaaamzltcqysz4qply7f3ww4rc446f7anfx3iw4y4ryxuz24rqsbwqta --wait-for-state AVAILABLE)
VOLUME_ID=$(echo "$VOLUME" | jq -r ".data.id")

echo volume: $VOLUME_ID
ATTACHMENT=$(oci compute volume-attachment attach --volume-id "$VOLUME_ID" --instance-id ocid1.instance.oc1.iad.anuwcljrtuanxnyclnv43tq4loqvx666zo2u4iorrydngdnoammhqs5d4gkq --type iscsi --device /dev/oracleoci/oraclevdc --wait-for-state ATTACHED)
echo "$ATTACHMENT"

IP=$(echo "$ATTACHMENT" | jq -r ".data.ipv4")
IQN=$(echo "$ATTACHMENT" | jq -r ".data.iqn")
PORT=$(echo "$ATTACHMENT" | jq -r ".data.port")

echo ip: $IP
echo iqn: $IQN
echo port: $PORT

ssh opc@129.213.90.154 "sudo iscsiadm -m node -o new -T $IQN -p $IP:$PORT"
ssh opc@129.213.90.154 "sudo iscsiadm -m node -o update -T $IQN -n node.startup -v automatic"
ssh opc@129.213.90.154 "sudo iscsiadm -m node -T $IQN -p $IP:$PORT -l"

ssh opc@129.213.90.154 "sudo parted /dev/sdc mklabel gpt"
ssh opc@129.213.90.154 "sudo parted -a opt /dev/sdc mkpart primary ext4 0% 100%"
ssh opc@129.213.90.154 "sudo mkfs.ext4 /dev/sdc1"
ssh opc@129.213.90.154 "sudo mkdir /mnt/block-3"
ssh opc@129.213.90.154 "sudo mount /dev/sdc1 /mnt/block-3"
ssh opc@129.213.90.154 "sudo chmod 777 /mnt/block-3"
