// p1-serial: reads a DSMR (Dutch/Belgian P1) telegram stream over TCP
// (e.g. ser2net or a P1 Wi-Fi dongle) and answers SolarEdge Modbus RTU
// meter polls on a USB-RS485 adapter, emulating a WattNode-style meter
// using the register layout of nmakel/solaredge_meterproxy.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/goburrow/serial"
)

type Config struct {
	P1ServerAddress string `json:"p1_server_address"`
	SerialPort      string `json:"serial_port"`
	MeterID         uint8  `json:"meter_id"`
	MeterSerial     string `json:"meter_serial"`
	SwapL1L3        bool   `json:"swap_l1_l3"`
	Debug           bool   `json:"debug"`
}

var (
	registryMutex sync.Mutex
	appConfig     Config

	// Moving-average "demand" values (180-sample window, as in nmakel)
	p1DemandPowerActive   float32
	p1L1DemandPowerActive float32
	p1L2DemandPowerActive float32
	p1L3DemandPowerActive float32

	demandQueue   []float32
	demandL1Queue []float32
	demandL2Queue []float32
	demandL3Queue []float32

	// Register map, addressed absolutely (nmakel layout)
	sunspecRegisters [3000]uint16

	// Raw P1 tariff counters (kWh)
	importT1, importT2 float64
	exportT1, exportT2 float64

	// Live values exposed via Modbus
	p1EnergyActive       float32
	p1ImportEnergyActive float32
	p1ExportEnergyActive float32
	p1PowerActive        float32
	p1L1PowerActive      float32
	p1L2PowerActive      float32
	p1L3PowerActive      float32
	p1VoltageL1          float32
	p1VoltageL2          float32
	p1VoltageL3          float32

	p1TelegramBuffer []string
)

func main() {
	configPath := flag.String("config", "config.json", "path to config.json")
	flag.Parse()

	log.Println("=== Starting SolarEdge P1 (DSMR) -> Modbus RTU meter proxy ===")

	loadConfig(*configPath)
	initSunSpecRegisters()

	go startP1StreamReader(appConfig.P1ServerAddress)
	go startModbusRegistrySyncer()

	if appConfig.Debug {
		go startStatusConsoleLogger()
	}

	startRTUServer(appConfig.SerialPort, appConfig.MeterID)
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

func initSunSpecRegisters() {
	registryMutex.Lock()
	defer registryMutex.Unlock()

	for i := range sunspecRegisters {
		sunspecRegisters[i] = 0
	}

	// WattNode base config (same values as the Python emulation)
	sunspecRegisters[1601] = 5 // CT rated current
	sunspecRegisters[1602] = 5
	sunspecRegisters[1603] = 5
	sunspecRegisters[1604] = 5
	sunspecRegisters[1605] = 0      // CT direction inversion
	sunspecRegisters[1606] = 1      // measurement averaging
	sunspecRegisters[1607] = 0      // power scale
	sunspecRegisters[1608] = 15     // demand period
	sunspecRegisters[1609] = 1      // demand subintervals
	sunspecRegisters[1610] = 10000  // power adjustment L1
	sunspecRegisters[1611] = 10000  // power adjustment L2
	sunspecRegisters[1612] = 10000  // power adjustment L3
	sunspecRegisters[1613] = 0xFFC8 // phase angle L1
	sunspecRegisters[1614] = 0xFFC8 // phase angle L2
	sunspecRegisters[1615] = 0xFFC8 // phase angle L3
	sunspecRegisters[1616] = 1500   // minimum power reading
	sunspecRegisters[1617] = 120    // phase offset
	sunspecRegisters[1618] = 20000  // current scale

	sunspecRegisters[1651] = 0 // apply config
	sunspecRegisters[1652] = 3 // Modbus address
	sunspecRegisters[1653] = 4 // baud rate code (4 = 9600)

	// Serial number and model
	serialNum, err := strconv.ParseInt(appConfig.MeterSerial, 10, 32)
	if err != nil {
		log.Printf("[CONFIG] meter_serial %q is not numeric, using 0", appConfig.MeterSerial)
	}
	sunspecRegisters[1701] = uint16(serialNum >> 16)
	sunspecRegisters[1702] = uint16(serialNum & 0xFFFF)
	sunspecRegisters[1707] = 202 // WattNode model 202
	sunspecRegisters[1708] = 31  // firmware version 3.1
}

// startP1StreamReader keeps a TCP connection to the P1 source and feeds
// every line to the DSMR parser, reconnecting on errors.
func startP1StreamReader(addr string) {
	for {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			log.Printf("[P1 ERROR] Connection failed: %v. Retrying in 5s...", err)
			time.Sleep(5 * time.Second)
			continue
		}
		log.Println("[P1] Connected to P1 stream.")
		reader := bufio.NewReader(conn)
		for {
			conn.SetReadDeadline(time.Now().Add(15 * time.Second))
			line, err := reader.ReadString('\n')
			if err != nil {
				log.Printf("[P1 ERROR] Connection lost: %v. Reconnecting...", err)
				break
			}
			parseDSMRLine(line)
		}
		conn.Close()
		time.Sleep(2 * time.Second)
	}
}

func parseDSMRLine(line string) {
	registryMutex.Lock()
	defer registryMutex.Unlock()

	cleanLine := strings.TrimSpace(line)

	// Telegram start: "/XXX5..."
	if strings.HasPrefix(cleanLine, "/") {
		p1TelegramBuffer = []string{cleanLine}
		return
	}

	p1TelegramBuffer = append(p1TelegramBuffer, cleanLine)

	// Telegram end: "!" optionally followed by CRC (e.g. "!AF35").
	// Note: the CRC is not validated.
	if !strings.Contains(cleanLine, "!") {
		return
	}

	var localL1Power, localL2Power, localL3Power float32
	var localImpT1, localImpT2, localExpT1, localExpT2 float64

	for _, bLine := range p1TelegramBuffer {
		// Cumulative energy counters (kWh)
		if strings.Contains(bLine, "1-0:1.8.1") {
			localImpT1 = extractDSMRValue(bLine)
		}
		if strings.Contains(bLine, "1-0:1.8.2") {
			localImpT2 = extractDSMRValue(bLine)
		}
		if strings.Contains(bLine, "1-0:2.8.1") {
			localExpT1 = extractDSMRValue(bLine)
		}
		if strings.Contains(bLine, "1-0:2.8.2") {
			localExpT2 = extractDSMRValue(bLine)
		}

		// Voltages
		if strings.Contains(bLine, "1-0:32.7.0") {
			p1VoltageL1 = float32(extractDSMRValue(bLine))
		}
		if strings.Contains(bLine, "1-0:52.7.0") {
			p1VoltageL2 = float32(extractDSMRValue(bLine))
		}
		if strings.Contains(bLine, "1-0:72.7.0") {
			p1VoltageL3 = float32(extractDSMRValue(bLine))
		}

		// Per-phase power: import positive, export negative (kW -> W)
		if strings.Contains(bLine, "1-0:21.7.0") {
			if val := float32(extractDSMRValue(bLine) * 1000.0); val > 0 {
				localL1Power = val
			}
		}
		if strings.Contains(bLine, "1-0:22.7.0") {
			if val := float32(extractDSMRValue(bLine) * 1000.0); val > 0 {
				localL1Power = -val
			}
		}
		if strings.Contains(bLine, "1-0:41.7.0") {
			if val := float32(extractDSMRValue(bLine) * 1000.0); val > 0 {
				localL2Power = val
			}
		}
		if strings.Contains(bLine, "1-0:42.7.0") {
			if val := float32(extractDSMRValue(bLine) * 1000.0); val > 0 {
				localL2Power = -val
			}
		}
		if strings.Contains(bLine, "1-0:61.7.0") {
			if val := float32(extractDSMRValue(bLine) * 1000.0); val > 0 {
				localL3Power = val
			}
		}
		if strings.Contains(bLine, "1-0:62.7.0") {
			if val := float32(extractDSMRValue(bLine) * 1000.0); val > 0 {
				localL3Power = -val
			}
		}
	}

	// Commit counters (ignore missing/zero lines)
	if localImpT1 > 0 {
		importT1 = localImpT1
	}
	if localImpT2 > 0 {
		importT2 = localImpT2
	}
	if localExpT1 > 0 {
		exportT1 = localExpT1
	}
	if localExpT2 > 0 {
		exportT2 = localExpT2
	}

	// Energy is passed through in kWh (this is what worked on the inverter)
	if importT1 > 0 && importT2 > 0 {
		p1ImportEnergyActive = float32(importT1 + importT2)
	}
	if exportT1 > 0 && exportT2 > 0 {
		p1ExportEnergyActive = float32(exportT1 + exportT2)
	}
	p1EnergyActive = p1ImportEnergyActive + p1ExportEnergyActive

	// Optional L1<->L3 swap for installations where the meter's phase
	// numbering doesn't match the inverter's.
	if appConfig.SwapL1L3 {
		p1L1PowerActive = localL3Power
		p1L3PowerActive = localL1Power
	} else {
		p1L1PowerActive = localL1Power
		p1L3PowerActive = localL3Power
	}
	p1L2PowerActive = localL2Power
	p1PowerActive = p1L1PowerActive + p1L2PowerActive + p1L3PowerActive

	demandQueue, p1DemandPowerActive = updateMovingAverage(demandQueue, p1PowerActive)
	demandL1Queue, p1L1DemandPowerActive = updateMovingAverage(demandL1Queue, p1L1PowerActive)
	demandL2Queue, p1L2DemandPowerActive = updateMovingAverage(demandL2Queue, p1L2PowerActive)
	demandL3Queue, p1L3DemandPowerActive = updateMovingAverage(demandL3Queue, p1L3PowerActive)

	p1TelegramBuffer = nil
}

func startStatusConsoleLogger() {
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		registryMutex.Lock()
		a1, a2, a3 := phaseCurrents()
		log.Printf("[LIVE STATUS] Total: %.0f W | L1: %.1f V, %.2f A, %.0f W | L2: %.1f V, %.2f A, %.0f W | L3: %.1f V, %.2f A, %.0f W | Meter kWh: (Imp: %.1f, Exp: %.1f)",
			p1PowerActive, p1VoltageL1, a1, p1L1PowerActive,
			p1VoltageL2, a2, p1L2PowerActive,
			p1VoltageL3, a3, p1L3PowerActive,
			p1ImportEnergyActive, p1ExportEnergyActive)
		registryMutex.Unlock()
	}
}

// phaseCurrents estimates current as |P| / U (P1 has no reliable current).
// Caller must hold registryMutex.
func phaseCurrents() (a1, a2, a3 float32) {
	if p1VoltageL1 > 0 {
		a1 = float32(math.Abs(float64(p1L1PowerActive))) / p1VoltageL1
	}
	if p1VoltageL2 > 0 {
		a2 = float32(math.Abs(float64(p1L2PowerActive))) / p1VoltageL2
	}
	if p1VoltageL3 > 0 {
		a3 = float32(math.Abs(float64(p1L3PowerActive))) / p1VoltageL3
	}
	return
}

func extractDSMRValue(line string) float64 {
	start := strings.Index(line, "(")
	end := strings.Index(line, "*")
	if start != -1 && end != -1 && end > start {
		if val, err := strconv.ParseFloat(line[start+1:end], 64); err == nil {
			return val
		}
	}
	return 0
}

// setLocalFloat32Register writes a float32 low word first (word order
// "little"), which is what the SolarEdge inverter accepted in practice.
func setLocalFloat32Register(index int, value float32) {
	bits := math.Float32bits(value)
	sunspecRegisters[index] = uint16(bits & 0xFFFF)
	sunspecRegisters[index+1] = uint16(bits >> 16)
}

// startRTUServer is a minimal Modbus RTU slave: it only answers
// function code 3 (read holding registers) for the configured slave ID.
func startRTUServer(port string, targetSlaveID uint8) {
	log.Printf("[SERIAL] Opening port %s...", port)
	serialConfig := &serial.Config{
		Address: port, BaudRate: 9600, DataBits: 8, Parity: "N", StopBits: 1, Timeout: 100 * time.Millisecond,
	}

	portConn, err := serial.Open(serialConfig)
	if err != nil {
		log.Fatalf("[SERIAL ERROR] Could not open port: %v", err)
	}
	defer portConn.Close()

	readBuffer := make([]byte, 256)

	for {
		n, err := portConn.Read(readBuffer[0:1])
		if err != nil || n == 0 {
			continue
		}
		if readBuffer[0] != targetSlaveID {
			continue
		}

		// A read-holding-registers request is exactly 8 bytes
		bytesRead := 1
		startTime := time.Now()
		for bytesRead < 8 && time.Since(startTime) < 50*time.Millisecond {
			n, _ := portConn.Read(readBuffer[bytesRead:8])
			bytesRead += n
		}
		if bytesRead < 8 || !validateCRC16(readBuffer[0:8]) {
			continue
		}

		functionCode := readBuffer[1]
		registerAddress := int(uint16(readBuffer[2])<<8 | uint16(readBuffer[3]))
		qty := int(uint16(readBuffer[4])<<8 | uint16(readBuffer[5]))

		if functionCode != 3 {
			continue
		}

		if appConfig.Debug {
			log.Printf("[MODBUS] SolarEdge reads address %d, count %d", registerAddress, qty)
		}

		registryMutex.Lock()
		if registerAddress+qty > len(sunspecRegisters) {
			registryMutex.Unlock()
			continue
		}

		byteCount := byte(qty * 2)
		rtuResponse := make([]byte, 3+int(byteCount))
		rtuResponse[0] = targetSlaveID
		rtuResponse[1] = functionCode
		rtuResponse[2] = byteCount

		for i := 0; i < qty; i++ {
			regVal := sunspecRegisters[registerAddress+i]
			rtuResponse[3+(i*2)] = byte(regVal >> 8)
			rtuResponse[3+(i*2)+1] = byte(regVal & 0xFF)
		}
		registryMutex.Unlock()

		crc := calculateCRC16(rtuResponse)
		rtuResponse = append(rtuResponse, byte(crc&0xFF), byte(crc>>8))

		portConn.Write(rtuResponse)
	}
}

func calculateCRC16(buffer []byte) uint16 {
	var crc uint16 = 0xFFFF
	for _, b := range buffer {
		crc ^= uint16(b)
		for i := 0; i < 8; i++ {
			if (crc & 0x0001) != 0 {
				crc = (crc >> 1) ^ 0xA001
			} else {
				crc >>= 1
			}
		}
	}
	return crc
}

func validateCRC16(buffer []byte) bool {
	if len(buffer) < 2 {
		return false
	}
	calculated := calculateCRC16(buffer[:len(buffer)-2])
	received := uint16(buffer[len(buffer)-1])<<8 | uint16(buffer[len(buffer)-2])
	return calculated == received
}

func startModbusRegistrySyncer() {
	ticker := time.NewTicker(1 * time.Second)
	for range ticker.C {
		registryMutex.Lock()

		var avgVoltage float32
		if p1VoltageL1 > 0 && p1VoltageL2 > 0 && p1VoltageL3 > 0 {
			avgVoltage = (p1VoltageL1 + p1VoltageL2 + p1VoltageL3) / 3.0
		}
		a1, a2, a3 := phaseCurrents()
		var pf float32 = 1.0

		// --- Block 1000 ---
		setLocalFloat32Register(1000, p1EnergyActive)       // total active energy
		setLocalFloat32Register(1002, p1ImportEnergyActive) // imported active energy
		setLocalFloat32Register(1004, p1EnergyActive)       // total active energy non-reset
		setLocalFloat32Register(1006, p1ImportEnergyActive) // imported active energy non-reset
		setLocalFloat32Register(1008, p1PowerActive)        // total power
		setLocalFloat32Register(1010, p1L1PowerActive)      // power L1
		setLocalFloat32Register(1012, p1L2PowerActive)      // power L2
		setLocalFloat32Register(1014, p1L3PowerActive)      // power L3
		setLocalFloat32Register(1016, avgVoltage)           // L-N voltage
		setLocalFloat32Register(1018, p1VoltageL1)          // L1-N voltage
		setLocalFloat32Register(1020, p1VoltageL2)          // L2-N voltage
		setLocalFloat32Register(1022, p1VoltageL3)          // L3-N voltage
		setLocalFloat32Register(1024, 0.0)                  // L-L voltage (n/a)
		setLocalFloat32Register(1026, 0.0)                  // L1-L2 voltage (n/a)
		setLocalFloat32Register(1028, 0.0)                  // L2-L3 voltage (n/a)
		setLocalFloat32Register(1030, 0.0)                  // L3-L1 voltage (n/a)
		setLocalFloat32Register(1032, 50.0)                 // line frequency (fixed)

		// The inverter polls "address 1038, count 6" continuously; these
		// registers are what it actually uses for live per-phase power.
		setLocalFloat32Register(1036, p1PowerActive)
		setLocalFloat32Register(1038, p1L1PowerActive)
		setLocalFloat32Register(1040, p1L2PowerActive)
		setLocalFloat32Register(1042, p1L3PowerActive)
		setLocalFloat32Register(1044, p1VoltageL3) // padding in case it reads further
		setLocalFloat32Register(1046, p1VoltageL3)
		setLocalFloat32Register(1048, p1VoltageL2)
		setLocalFloat32Register(1050, p1VoltageL1)

		// --- Block 1100 ---
		setLocalFloat32Register(1100, p1EnergyActive)       // total active energy L1
		setLocalFloat32Register(1102, 0)                    // total active energy L2
		setLocalFloat32Register(1104, 0)                    // total active energy L3
		setLocalFloat32Register(1106, p1ImportEnergyActive) // imported active energy L1
		setLocalFloat32Register(1108, 0)                    // imported active energy L2
		setLocalFloat32Register(1110, 0)                    // imported active energy L3
		setLocalFloat32Register(1112, p1ExportEnergyActive) // total exported active energy
		setLocalFloat32Register(1114, p1ExportEnergyActive) // total exported active energy non-reset
		setLocalFloat32Register(1116, p1ExportEnergyActive) // exported energy L1
		setLocalFloat32Register(1118, 0)                    // exported energy L2
		setLocalFloat32Register(1120, 0)                    // exported energy L3

		// Reactive & apparent energy (n/a)
		for addr := 1122; addr <= 1136; addr += 2 {
			setLocalFloat32Register(addr, 0.0)
		}

		// Power factors
		setLocalFloat32Register(1138, pf)
		setLocalFloat32Register(1140, pf)
		setLocalFloat32Register(1142, pf)
		setLocalFloat32Register(1144, pf)

		// Reactive & apparent power (n/a)
		for addr := 1146; addr <= 1160; addr += 2 {
			setLocalFloat32Register(addr, 0.0)
		}

		// Currents
		setLocalFloat32Register(1162, a1)
		setLocalFloat32Register(1164, a2)
		setLocalFloat32Register(1166, a3)

		// Demand power
		setLocalFloat32Register(1168, p1DemandPowerActive)   // demand power
		setLocalFloat32Register(1170, 0.0)                   // minimum demand power
		setLocalFloat32Register(1172, 0.0)                   // maximum demand power
		setLocalFloat32Register(1174, 0.0)                   // apparent demand power
		setLocalFloat32Register(1176, p1L1DemandPowerActive) // demand power L1
		setLocalFloat32Register(1178, p1L2DemandPowerActive) // demand power L2
		setLocalFloat32Register(1180, p1L3DemandPowerActive) // demand power L3

		registryMutex.Unlock()
	}
}
