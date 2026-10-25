# solaredge-meter-proxy

Two small Go programs that make a SolarEdge inverter believe it has its own
energy meter, while the data actually comes from a meter you already have
(Dutch/Belgian P1 smart meter, or any meter whose readings end up on MQTT).

The inverter then does export limitation / consumption monitoring / battery
control based on that data — without buying and wiring an extra
SolarEdge-compatible meter.

The register layout follows the WattNode emulation of
[nmakel/solaredge_meterproxy](https://github.com/nmakel/solaredge_meterproxy),
rewritten as single static binaries.

> **Use at your own risk.** This is a hobby project that works on my
> installation. It is shared as-is, with little to no support.
> Wrong meter data can make your inverter export more (or less) than
> intended — verify the readings in the SolarEdge monitoring / SetApp
> before relying on export limitation.

## Variants

| Directory    | Input                                   | Output to inverter                |
|--------------|-----------------------------------------|-----------------------------------|
| `p1-serial/` | DSMR P1 telegrams over TCP (e.g. ser2net, P1 Wi-Fi dongle with raw TCP) | Modbus **RTU** via a USB-RS485 adapter wired to the inverter's RS485 meter port |
| `mqtt-tcp/`  | JSON on an MQTT topic (HomeWizard `/api/v1/data` format) | Modbus **TCP** server the inverter connects to over LAN |

```
p1-serial:  P1 meter ──TCP──▶ p1-serial ──RS485 (USB adapter)──▶ SolarEdge
mqtt-tcp:   any meter ──MQTT──▶ mqtt-tcp ◀──Modbus TCP── SolarEdge
```

## Build

Requires Go 1.21+.

```bash
git clone https://github.com/yaldair/solaredge-meter-proxy.git
cd solaredge-meter-proxy
go mod tidy
go build -o bin/ ./...
```

Cross-compile for e.g. a Raspberry Pi:

```bash
GOOS=linux GOARCH=arm64 go build -o bin/ ./...
```

## Configuration

Each binary reads `config.json` from the working directory, or use
`-config /path/to/config.json`. Copy the `config.example.json` from the
variant's directory.

### p1-serial

```json
{
  "p1_server_address": "192.168.1.50:23",
  "serial_port": "/dev/ttyACM0",
  "meter_id": 3,
  "meter_serial": "12345678",
  "swap_l1_l3": false,
  "debug": false
}
```

| Key | Meaning |
|---|---|
| `p1_server_address` | `host:port` of a raw TCP stream of P1 telegrams (ser2net, dongle) |
| `serial_port` | USB-RS485 adapter connected to the inverter (fixed 9600 8N1) |
| `meter_id` | Modbus slave ID the inverter polls — must match the meter's device ID in SetApp |
| `meter_serial` | Numeric serial reported to the inverter; non-numeric → 0 |
| `swap_l1_l3` | Swap L1/L3 power if the meter's phase order doesn't match the inverter's |
| `debug` | Log every Modbus request and a live status line each second |

Notes:
- Energy counters are summed from tariff 1 + tariff 2 (`1.8.1`/`1.8.2`,
  `2.8.1`/`2.8.2`) and are only updated once **both** tariffs are non-zero.
- P1 has no per-phase current, so current is estimated as |P| / U.
- Frequency is fixed at 50 Hz, power factor at 1.0.
- The P1 CRC is not checked.

### mqtt-tcp

```json
{
  "mqtt_broker": "tcp://192.168.1.10:1883",
  "mqtt_topic": "garage_meter/api",
  "mqtt_username": "",
  "mqtt_password": "",
  "mqtt_client_id": "solaredge-mqtt-proxy",
  "modbus_tcp_port": ":502",
  "meter_id": 3,
  "meter_serial": "12345678",
  "debug": false
}
```

The payload on the topic must be JSON with HomeWizard-style field names,
for example:

```json
{
  "total_power_import_kwh": 1234.5,
  "total_power_export_kwh": 678.9,
  "active_power_w": -1500,
  "active_power_l1_w": -500,
  "active_power_l2_w": -500,
  "active_power_l3_w": -500,
  "active_voltage_l1_v": 230.1,
  "active_voltage_l2_v": 231.0,
  "active_voltage_l3_v": 229.8,
  "active_current_l1_a": 2.2,
  "active_current_l2_a": 2.2,
  "active_current_l3_a": 2.2,
  "active_frequency_hz": 50.0
}
```

Missing fields are treated as 0. Any source works (HomeWizard polled by
Node-RED/Home Assistant and republished, a Tasmota/ESPHome meter, ...),
as long as the field names match. Positive power = import, negative = export.

The Modbus TCP server answers every unit ID; `meter_id` is informational.
Binding port 502 needs root or `CAP_NET_BIND_SERVICE` (see the example
systemd unit).

## Running as a service

Example systemd units are in [`examples/`](examples/):

```bash
sudo cp examples/se-p1-serial.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now se-p1-serial
journalctl -u se-p1-serial -f
```

## Technical notes

- Float32 values are written **low word first** (word order "little"); this
  is what the SolarEdge inverter accepted in practice.
- The inverter continuously polls a short block around register 1038
  (per-phase power); the full 1000/1100 blocks are filled as in nmakel's
  WattNode layout.
- "Demand" values are a 180-sample moving average.
- Only Modbus function code 3 (read holding registers) is implemented.

## Credits

- Register layout: [nmakel/solaredge_meterproxy](https://github.com/nmakel/solaredge_meterproxy)
- Written by Yaldair, with help from AI assistants (Claude, Mistral).

## License

MIT — see [LICENSE](LICENSE).
