// mqtt-tcp: subscribes to an MQTT topic carrying HomeWizard-style meter JSON
// and serves it as a Modbus TCP meter for a SolarEdge inverter, using the
// WattNode-style register layout of nmakel/solaredge_meterproxy.
package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type Config struct {
	MQTTBroker    string `json:"mqtt_broker"`
	MQTTTopic     string `json:"mqtt_topic"`
	MQTTUsername  string `json:"mqtt_username,omitempty"`
	MQTTPassword  string `json:"mqtt_password,omitempty"`
	MQTTClientID  string `json:"mqtt_client_id,omitempty"`
	ModbusTCPPort string `json:"modbus_tcp_port"`
	MeterID       uint8  `json:"meter_id"` // currently informational: all unit IDs are answered
	MeterSerial   string `json:"meter_serial"`
	Debug         bool   `json:"debug"`
}

// MeterData matches the JSON of the HomeWizard local API (/api/v1/data)
// as published on MQTT. Missing fields simply stay 0.
type MeterData struct {
	TotalPowerImportKWh      float64 `json:"total_power_import_kwh"`
	TotalPowerImportT1KWh    float64 `json:"total_power_import_t1_kwh"`
	TotalPowerExportKWh      float64 `json:"total_power_export_kwh"`
	TotalPowerExportT1KWh    float64 `json:"total_power_export_t1_kwh"`
	ActivePowerW             float64 `json:"active_power_w"`
	ActivePowerL1W           float64 `json:"active_power_l1_w"`
	ActivePowerL2W           float64 `json:"active_power_l2_w"`
	ActivePowerL3W           float64 `json:"active_power_l3_w"`
	ActiveVoltageL1V         float64 `json:"active_voltage_l1_v"`
	ActiveVoltageL2V         float64 `json:"active_voltage_l2_v"`
	ActiveVoltageL3V         float64 `json:"active_voltage_l3_v"`
	ActiveCurrentL1A         float64 `json:"active_current_l1_a"`
	ActiveCurrentL2A         float64 `json:"active_current_l2_a"`
	ActiveCurrentL3A         float64 `json:"active_current_l3_a"`
	ActiveApparentPowerVA    float64 `json:"active_apparent_power_va"`
	ActiveApparentPowerL1VA  float64 `json:"active_apparent_power_l1_va"`
	ActiveApparentPowerL2VA  float64 `json:"active_apparent_power_l2_va"`
	ActiveApparentPowerL3VA  float64 `json:"active_apparent_power_l3_va"`
	ActiveReactivePowerVAR   float64 `json:"active_reactive_power_var"`
	ActiveReactivePowerL1VAR float64 `json:"active_reactive_power_l1_var"`
	ActiveReactivePowerL2VAR float64 `json:"active_reactive_power_l2_var"`
	ActiveReactivePowerL3VAR float64 `json:"active_reactive_power_l3_var"`
	ActivePowerFactorL1      float64 `json:"active_power_factor_l1"`
	ActivePowerFactorL2      float64 `json:"active_power_factor_l2"`
	ActivePowerFactorL3      float64 `json:"active_power_factor_l3"`
	ActiveFrequencyHz        float64 `json:"active_frequency_hz"`
}

var (
	registryMutex    sync.Mutex
	appConfig        Config
	sunspecRegisters [3000]uint16

	// Live data from MQTT
	energyActive       float32
	importEnergyActive float32
	exportEnergyActive float32
	powerActive        float32
	l1PowerActive      float32
	l2PowerActive      float32
	l3PowerActive      float32
	voltageL1          float32
	voltageL2          float32
	voltageL3          float32
	currentL1          float32
	currentL2          float32
	currentL3          float32
	apparentPower      float32
	apparentPowerL1    float32
	apparentPowerL2    float32
	apparentPowerL3    float32
	reactivePower      float32
	reactivePowerL1    float32
	reactivePowerL2    float32
	reactivePowerL3    float32
	powerFactorL1      float32
	powerFactorL2      float32
	powerFactorL3      float32
	frequency          float32

	// Moving-average "demand" values (180 samples)
	demandPowerActive   float32
	l1DemandPowerActive float32
	l2DemandPowerActive float32
	l3DemandPowerActive float32
	demandQueue         []float32
	demandL1Queue       []float32
	demandL2Queue       []float32
	demandL3Queue       []float32
)

func main() {
	configPath := flag.String("config", "config.json", "path to config.json")
	flag.Parse()

	log.Println("=== Starting SolarEdge MQTT -> Modbus TCP meter proxy ===")

	loadConfig(*configPath)
	initSunSpecRegisters()

	go startMQTTClient()
	go startModbusRegistrySyncer()
	go startModbusTCPServer()

	if appConfig.Debug {
		go startStatusConsoleLogger()
	}

	select {}
}

func loadConfig(path string) {
	configFile, err := os.Open(path)
	if err != nil {
		log.Fatalf("Could not open %s: %v", path, err)
	}
	defer configFile.Close()

	if err := json.NewDecoder(configFile).Decode(&appConfig); err != nil {
		log.Fatalf("Error parsing %s: %v", path, err)
	}
	if appConfig.MQTTClientID == "" {
		appConfig.MQTTClientID = "solaredge-mqtt-proxy"
	}
}

func initSunSpecRegisters() {
	registryMutex.Lock()
	defer registryMutex.Unlock()

	for i := range sunspecRegisters {
		sunspecRegisters[i] = 0
	}

	// Registers 0-60: ASCII placeholder text. This is NOT a valid SunSpec
	// common/meter model; it was left in because the inverter works with it
	// and doesn't appear to read this range.
	sunspecRegisters[0] = 1
	sunspecRegisters[1] = 66
	writeASCII(2, "SUNSSF")
	writeASCII(20, "SunSpec Meter")
	sunspecRegisters[40] = 1
	sunspecRegisters[41] = 26
	writeASCII(42, "SunSpec Meter Model")

	// WattNode base config
	sunspecRegisters[1601] = 5 // CT rated current
	sunspecRegisters[1602] = 5
	sunspecRegisters[1603] = 5
	sunspecRegisters[1604] = 5
	sunspecRegisters[1605] = 0  // CT direction inversion
	sunspecRegisters[1606] = 1  // measurement averaging
	sunspecRegisters[1607] = 0  // power scale
	sunspecRegisters[1608] = 15 // demand period
	sunspecRegisters[1609] = 1  // demand subintervals

	// Serial number
	serialNum, err := strconv.ParseInt(appConfig.MeterSerial, 10, 32)
	if err != nil {
		log.Printf("[CONFIG] meter_serial %q is not numeric, using 0", appConfig.MeterSerial)
	}
	sunspecRegisters[1701] = uint16(serialNum >> 16)
	sunspecRegisters[1702] = uint16(serialNum & 0xFFFF)
}

// writeASCII stores one character per register (as the original did).
func writeASCII(start int, s string) {
	for i, c := range s {
		sunspecRegisters[start+i] = uint16(c)
	}
}

func startMQTTClient() {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(appConfig.MQTTBroker)
	opts.SetClientID(appConfig.MQTTClientID)
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(5 * time.Second)

	if appConfig.MQTTUsername != "" && appConfig.MQTTPassword != "" {
		opts.SetUsername(appConfig.MQTTUsername)
		opts.SetPassword(appConfig.MQTTPassword)
	}

	// (Re)subscribe on every connect so subscriptions survive reconnects
	opts.SetOnConnectHandler(func(client mqtt.Client) {
		log.Println("[MQTT] Connected to broker.")
		token := client.Subscribe(appConfig.MQTTTopic, 0, func(_ mqtt.Client, msg mqtt.Message) {
			parseMQTTMessage(msg.Payload())
		})
		if token.Wait() && token.Error() != nil {
			log.Printf("[MQTT] Subscription failed: %v", token.Error())
			return
		}
		log.Printf("[MQTT] Subscribed to topic: %s", appConfig.MQTTTopic)
	})

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("[MQTT] Connection failed: %v", token.Error())
	}
}

func parseMQTTMessage(payload []byte) {
	var data MeterData
	if err := json.Unmarshal(payload, &data); err != nil {
		log.Printf("[MQTT] Error parsing JSON: %v", err)
		return
	}

	registryMutex.Lock()
	defer registryMutex.Unlock()

	powerActive = float32(data.ActivePowerW)
	l1PowerActive = float32(data.ActivePowerL1W)
	l2PowerActive = float32(data.ActivePowerL2W)
	l3PowerActive = float32(data.ActivePowerL3W)

	voltageL1 = float32(data.ActiveVoltageL1V)
	voltageL2 = float32(data.ActiveVoltageL2V)
	voltageL3 = float32(data.ActiveVoltageL3V)

	currentL1 = float32(data.ActiveCurrentL1A)
	currentL2 = float32(data.ActiveCurrentL2A)
	currentL3 = float32(data.ActiveCurrentL3A)

	apparentPower = float32(data.ActiveApparentPowerVA)
	apparentPowerL1 = float32(data.ActiveApparentPowerL1VA)
	apparentPowerL2 = float32(data.ActiveApparentPowerL2VA)
	apparentPowerL3 = float32(data.ActiveApparentPowerL3VA)

	reactivePower = float32(data.ActiveReactivePowerVAR)
	reactivePowerL1 = float32(data.ActiveReactivePowerL1VAR)
	reactivePowerL2 = float32(data.ActiveReactivePowerL2VAR)
	reactivePowerL3 = float32(data.ActiveReactivePowerL3VAR)

	powerFactorL1 = float32(data.ActivePowerFactorL1)
	powerFactorL2 = float32(data.ActivePowerFactorL2)
	powerFactorL3 = float32(data.ActivePowerFactorL3)

	frequency = float32(data.ActiveFrequencyHz)

	// Energy is passed through in kWh (this is what worked on the inverter)
	importEnergyActive = float32(data.TotalPowerImportKWh)
	exportEnergyActive = float32(data.TotalPowerExportKWh)
	energyActive = importEnergyActive + exportEnergyActive

	demandQueue, demandPowerActive = updateMovingAverage(demandQueue, powerActive)
	demandL1Queue, l1DemandPowerActive = updateMovingAverage(demandL1Queue, l1PowerActive)
	demandL2Queue, l2DemandPowerActive = updateMovingAverage(demandL2Queue, l2PowerActive)
	demandL3Queue, l3DemandPowerActive = updateMovingAverage(demandL3Queue, l3PowerActive)

	if appConfig.Debug {
		log.Printf("[MQTT] Update: Power=%.1f W, L1=%.1f W, L2=%.1f W, L3=%.1f W",
			powerActive, l1PowerActive, l2PowerActive, l3PowerActive)
	}
}

func updateMovingAverage(queue []float32, newVal float32) ([]float32, float32) {
	queue = append(queue, newVal)
	if len(queue) > 180 {
		queue = queue[1:]
	}
	var sum float32
	for _, v := range queue {
		sum += v
	}
	return queue, sum / float32(len(queue))
}

func startModbusTCPServer() {
	listener, err := net.Listen("tcp", appConfig.ModbusTCPPort)
	if err != nil {
		log.Fatalf("[MODBUS-TCP] Failed to start server: %v", err)
	}
	defer listener.Close()

	log.Printf("[MODBUS-TCP] Listening on %s", appConfig.ModbusTCPPort)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("[MODBUS-TCP] Accept error: %v", err)
			continue
		}
		go handleModbusConnection(conn)
	}
}

// handleModbusConnection is a minimal Modbus TCP slave: function code 3 only.
// It assumes one request per read, which is how the inverter behaves.
func handleModbusConnection(conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, 260)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			log.Printf("[MODBUS-TCP] Read error: %v", err)
			return
		}
		// MBAP header (6) + unit ID + function code + address (2) + quantity (2)
		if n < 12 {
			log.Printf("[MODBUS-TCP] Invalid request length: %d", n)
			return
		}

		transactionID := binary.BigEndian.Uint16(buffer[0:2])
		protocolID := binary.BigEndian.Uint16(buffer[2:4])
		unitID := buffer[6]
		functionCode := buffer[7]
		addr := binary.BigEndian.Uint16(buffer[8:10])
		qty := binary.BigEndian.Uint16(buffer[10:12])

		if functionCode != 3 {
			conn.Write(exceptionResponse(transactionID, unitID, functionCode, 0x01)) // illegal function
			continue
		}

		registryMutex.Lock()
		if int(addr)+int(qty) > len(sunspecRegisters) || qty > 125 {
			registryMutex.Unlock()
			conn.Write(exceptionResponse(transactionID, unitID, functionCode, 0x02)) // illegal data address
			continue
		}

		response := make([]byte, 9+int(qty)*2)
		binary.BigEndian.PutUint16(response[0:2], transactionID)
		binary.BigEndian.PutUint16(response[2:4], protocolID)
		binary.BigEndian.PutUint16(response[4:6], uint16(3+int(qty)*2))
		response[6] = unitID
		response[7] = 3
		response[8] = byte(qty * 2)

		for i := uint16(0); i < qty; i++ {
			binary.BigEndian.PutUint16(response[9+int(i)*2:11+int(i)*2], sunspecRegisters[addr+i])
		}
		registryMutex.Unlock()

		conn.Write(response)
	}
}

func exceptionResponse(transactionID uint16, unitID, functionCode, code byte) []byte {
	return []byte{
		byte(transactionID >> 8), byte(transactionID),
		0, 0, // protocol ID
		0, 3, // length
		unitID,
		functionCode | 0x80,
		code,
	}
}

func startModbusRegistrySyncer() {
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		registryMutex.Lock()

		var avgVoltage float32
		if voltageL1 > 0 && voltageL2 > 0 && voltageL3 > 0 {
			avgVoltage = (voltageL1 + voltageL2 + voltageL3) / 3.0
		}
		var avgPowerFactor float32
		if powerFactorL1 > 0 && powerFactorL2 > 0 && powerFactorL3 > 0 {
			avgPowerFactor = (powerFactorL1 + powerFactorL2 + powerFactorL3) / 3.0
		}

		// --- Block 1000: energy and power ---
		setLocalFloat32Register(1000, energyActive)       // total active energy
		setLocalFloat32Register(1002, importEnergyActive) // imported active energy
		setLocalFloat32Register(1004, energyActive)       // total active energy non-reset
		setLocalFloat32Register(1006, importEnergyActive) // imported active energy non-reset
		setLocalFloat32Register(1008, powerActive)        // total power (W)
		setLocalFloat32Register(1010, l1PowerActive)      // power L1 (W)
		setLocalFloat32Register(1012, l2PowerActive)      // power L2 (W)
		setLocalFloat32Register(1014, l3PowerActive)      // power L3 (W)
		setLocalFloat32Register(1016, avgVoltage)         // average L-N voltage
		setLocalFloat32Register(1018, voltageL1)          // L1-N voltage
		setLocalFloat32Register(1020, voltageL2)          // L2-N voltage
		setLocalFloat32Register(1022, voltageL3)          // L3-N voltage
		setLocalFloat32Register(1024, 0.0)                // L-L voltage (n/a)
		setLocalFloat32Register(1026, 0.0)                // L1-L2 voltage (n/a)
		setLocalFloat32Register(1028, 0.0)                // L2-L3 voltage (n/a)
		setLocalFloat32Register(1030, 0.0)                // L3-L1 voltage (n/a)
		setLocalFloat32Register(1032, frequency)          // line frequency

		// Registers the inverter polls continuously (address 1038, count 6)
		setLocalFloat32Register(1036, powerActive)
		setLocalFloat32Register(1038, l1PowerActive)
		setLocalFloat32Register(1040, l2PowerActive)
		setLocalFloat32Register(1042, l3PowerActive)
		setLocalFloat32Register(1044, voltageL1)
		setLocalFloat32Register(1046, voltageL2)
		setLocalFloat32Register(1048, voltageL3)

		// --- Block 1100: energy per phase ---
		setLocalFloat32Register(1100, energyActive)       // total active energy
		setLocalFloat32Register(1102, 0.0)                // total active energy L2 (n/a)
		setLocalFloat32Register(1104, 0.0)                // total active energy L3 (n/a)
		setLocalFloat32Register(1106, importEnergyActive) // imported active energy L1
		setLocalFloat32Register(1108, 0.0)                // imported active energy L2 (n/a)
		setLocalFloat32Register(1110, 0.0)                // imported active energy L3 (n/a)
		setLocalFloat32Register(1112, exportEnergyActive) // exported active energy
		setLocalFloat32Register(1114, exportEnergyActive) // exported active energy non-reset
		setLocalFloat32Register(1116, exportEnergyActive) // exported active energy L1
		setLocalFloat32Register(1118, 0.0)                // exported active energy L2 (n/a)
		setLocalFloat32Register(1120, 0.0)                // exported active energy L3 (n/a)

		// Reactive & apparent energy (n/a)
		for a := 1122; a <= 1136; a += 2 {
			setLocalFloat32Register(a, 0.0)
		}

		// Power factors
		setLocalFloat32Register(1138, avgPowerFactor)
		setLocalFloat32Register(1140, powerFactorL1)
		setLocalFloat32Register(1142, powerFactorL2)
		setLocalFloat32Register(1144, powerFactorL3)

		// Reactive & apparent power
		setLocalFloat32Register(1146, reactivePower)
		setLocalFloat32Register(1148, reactivePowerL1)
		setLocalFloat32Register(1150, reactivePowerL2)
		setLocalFloat32Register(1152, reactivePowerL3)
		setLocalFloat32Register(1154, apparentPower)
		setLocalFloat32Register(1156, apparentPowerL1)
		setLocalFloat32Register(1158, apparentPowerL2)
		setLocalFloat32Register(1160, apparentPowerL3)

		// Currents
		setLocalFloat32Register(1162, currentL1)
		setLocalFloat32Register(1164, currentL2)
		setLocalFloat32Register(1166, currentL3)

		// Demand power
		setLocalFloat32Register(1168, demandPowerActive)
		setLocalFloat32Register(1170, 0.0) // minimum demand power (n/a)
		setLocalFloat32Register(1172, 0.0) // maximum demand power (n/a)
		setLocalFloat32Register(1174, 0.0) // apparent demand power (n/a)
		setLocalFloat32Register(1176, l1DemandPowerActive)
		setLocalFloat32Register(1178, l2DemandPowerActive)
		setLocalFloat32Register(1180, l3DemandPowerActive)

		registryMutex.Unlock()
	}
}

// setLocalFloat32Register writes a float32 low word first (word order
// "little"), which is what the SolarEdge inverter accepted in practice.
func setLocalFloat32Register(index int, value float32) {
	bits := math.Float32bits(value)
	sunspecRegisters[index] = uint16(bits & 0xFFFF)
	sunspecRegisters[index+1] = uint16(bits >> 16)
}

func startStatusConsoleLogger() {
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		registryMutex.Lock()
		log.Printf(
			"[LIVE STATUS] Power: %.1f W | L1: %.1f W, %.1f V, %.2f A | L2: %.1f W, %.1f V, %.2f A | L3: %.1f W, %.1f V, %.2f A | Energy kWh: Imp=%.1f, Exp=%.1f | Freq: %.1f Hz",
			powerActive,
			l1PowerActive, voltageL1, currentL1,
			l2PowerActive, voltageL2, currentL2,
			l3PowerActive, voltageL3, currentL3,
			importEnergyActive, exportEnergyActive,
			frequency,
		)
		registryMutex.Unlock()
	}
}
