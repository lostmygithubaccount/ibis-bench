#find /dev/ | grep google-local-nvme-ssd

sudo mdadm --create /dev/md0 --level=0 --raid-devices=24 \
    /dev/disk/by-id/google-local-nvme-ssd-0 \
    /dev/disk/by-id/google-local-nvme-ssd-1 \
    /dev/disk/by-id/google-local-nvme-ssd-2 \
    /dev/disk/by-id/google-local-nvme-ssd-3 \
    /dev/disk/by-id/google-local-nvme-ssd-4 \
    /dev/disk/by-id/google-local-nvme-ssd-5 \
    /dev/disk/by-id/google-local-nvme-ssd-6 \
    /dev/disk/by-id/google-local-nvme-ssd-7 \
    /dev/disk/by-id/google-local-nvme-ssd-8 \
    /dev/disk/by-id/google-local-nvme-ssd-9 \
    /dev/disk/by-id/google-local-nvme-ssd-10 \
    /dev/disk/by-id/google-local-nvme-ssd-11 \
    /dev/disk/by-id/google-local-nvme-ssd-12 \
    /dev/disk/by-id/google-local-nvme-ssd-13 \
    /dev/disk/by-id/google-local-nvme-ssd-14 \
    /dev/disk/by-id/google-local-nvme-ssd-15 \
    /dev/disk/by-id/google-local-nvme-ssd-16 \
    /dev/disk/by-id/google-local-nvme-ssd-17 \
    /dev/disk/by-id/google-local-nvme-ssd-18 \
    /dev/disk/by-id/google-local-nvme-ssd-19 \
    /dev/disk/by-id/google-local-nvme-ssd-20 \
    /dev/disk/by-id/google-local-nvme-ssd-21 \
    /dev/disk/by-id/google-local-nvme-ssd-22 \
    /dev/disk/by-id/google-local-nvme-ssd-23

sudo mkfs.ext4 -F /dev/md0

sudo mkdir -p $HOME/nvme

