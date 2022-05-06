# binance dumper systemd unit file
This readme explain how to install *bitfinexdumper* into a linux server using systemd.  
One may prefer the docker/podman way.

## Installation
```bash
bash
set -euo pipefail
wget "https://github.com/maxisoft/binance-dumper/releases/download/v1.0.0/binance_dumper" -O "/tmp/binance_dumper"
useradd -r -s /bin/false binancedumper # TODO checks that the added user doesn't break your server security (like ssh)
install -d -m 0755 -o binancedumper -g binancedumper /opt/binancedumper
install -d -m 0755 -o binancedumper -g binancedumper /var/binancedumper
install -m 0554 -o binancedumper -g binancedumper /tmp/binance_dumper /opt/binancedumper/binance_dumper

# optionally edit the binancedumper.service file
cp binancedumper.service /etc/systemd/system/binancedumper.service
systemctl daemon-reload
systemctl start binancedumper
# check running state with:
# systemctl status binancedumper
systemctl enable binancedumper

# cleanup
rm -f /tmp/binance_dumper
```
