# Deployment on Armbian

Instructions and tools enabling deployment of Swift and ProxyFS on Armbian-based systems.

## Prerequisites

* One (evenutally or more) `arm7l`-based system with at least 2GB DRAM
* Default/single (root) file system deployed on adequate storage device supporting xttrs
* Hard/DHCP-assigned IPv4 Address of 10.0.0.100 (for first ODroid)
* A sudo-capable User (not root!) with GitHub access (e.g. ~/.ssh/id_rsa pre-registered)
* Open/routable TCP Ports for SSH (e.g. 22) and Swift (e.g. 8080)

## Example Components

* ODroid HC1 - ARM-based SBC w/ case for 2.5" HD/SSD - $64
* 5V/4A Power Adapter - POS 2.1mm inner, NEG 5.5mm outer barrel style - $16
* Samsung 64GB EVO microSD - $11
* Rocketek USB 3.0 microSD Adapter - both USB-A and USB-C versions available - $8-$9
* Samsung 1TB EVO 2.5" SATA SSD - $148

## Useful Tools

* Armbian Site: https://www.armbian.com/odroid-hc1/
    * Armbian Stretch: 4.14y "mainline" kernel based on Debian (used in this example)
    * Armbian Bionic: 4.14y "mainline" kernel based on Ubuntu
* The Unarchiver: https://thunarchiver.com
* Balena Etcher: https://www.balena.io/etcher

## Setup Instructions

* Initial login
    * ssh root@10.0.0.100 (initial pwd = "1234"; you will be forced to change it)
    * create non-root/sudo-capable user... let's call it "ed"
* ssh ed@10.0.0.100
    * sudo apt-get update
    * sudo apt-get upgrade
    * mkdir ~/.ssh
    * chmod 700 ~/.ssh
* From client
    * Ensure your (GitHub-registered) user has SSH Keys (~/.ssh/id_rsa}|.pub)
    * cd ~/.ssh
    * scp id_rsa.pub ed@10.0.0.100:.ssh/authorized_keys
    * scp id_rsa.pub ed@10.0.0.100:.ssh/id_rsa.pub
    * scp id_rsa ed@10.0.0.100:.ssh/id_rsa
* Shutdown ODroid (sudo shutdown -h now)
* Install SSD
* Reboot ODroid
* Migrade root drive from SCCard to SSD
    * ssh ed@10.0.0.100
        * sudo nand-sata-install
        * Select "Boot from SD"
        * Select "/dev/sda1"
        * Format as "btrfs"
        * Reboot (sudo shutdown -r now)
* ssh ed@10.0.0.100
    * sudo armbian-config
        * System->DTB->hc1
        * System->SSH->Uncheck "Allow root login"
        * System->SSH->Uncheck "Password login"
        * Personal->Timezone->US->Pacific-New
        * Personal->Hostname->Odroid0
* To add additional client
    * ssh ed@10.0.0.100 (from 1st client, since 2nd client can't SSH yet)
        * sudo-armbian-config
            * System->SSH->Check "Password login"
    * cd ~/.ssh (from 2nd client)
    * scp id_rsa.pub ed@10.0.0.100:.ssh/authorized_key_to_append
    * ssh ed@10.0.0.100
        * sudo armbian-config
            * System->SSH->Uncheck "Password login"
        * cd ~/.ssh
        * cat authorized_key_to_append >> authorized_keys
        * rm authorized_key_to_append

## Provision Swift & ProxyFS

* ssh ed@10.0.0.100
* cd /tmp
* wget https://github.com/swiftstack/ProxyFS/blob/development/armbian/provision.sh
* chmod +x provision.sh
* sudo ./provision.sh
