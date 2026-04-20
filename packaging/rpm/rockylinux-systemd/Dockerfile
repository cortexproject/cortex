FROM rockylinux:9
ENV container docker
RUN dnf -y install systemd && dnf clean all && \
        (cd /usr/lib/systemd/system/sysinit.target.wants/; for i in *; do [ "$i" = \
        systemd-tmpfiles-setup.service ] || rm -f "$i"; done); \
        rm -f /usr/lib/systemd/system/multi-user.target.wants/*; \
        rm -f /etc/systemd/system/*.wants/*; \
        rm -f /usr/lib/systemd/system/local-fs.target.wants/*; \
        rm -f /usr/lib/systemd/system/sockets.target.wants/*udev*; \
        rm -f /usr/lib/systemd/system/sockets.target.wants/*initctl*; \
        rm -f /usr/lib/systemd/system/basic.target.wants/*;

VOLUME [ "/sys/fs/cgroup"]
CMD ["/usr/sbin/init"]
