FROM alpine:3
RUN apk add --no-cache openssh-server \
 && adduser -D -s /bin/sh tester && passwd -u tester \
 && ssh-keygen -A \
 && sed -i -E '/^\s*(AllowTcpForwarding|AllowStreamLocalForwarding|PasswordAuthentication)/d' /etc/ssh/sshd_config \
 && printf 'AllowTcpForwarding yes\nPasswordAuthentication no\nPermitRootLogin no\nAllowStreamLocalForwarding yes\n' >> /etc/ssh/sshd_config
COPY entrypoint.sh /entrypoint.sh
CMD ["/bin/sh", "/entrypoint.sh"]
