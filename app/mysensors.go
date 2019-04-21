package main

import (
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/buxtronix/mysensors-prom"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tarm/serial"
)

var (
	addr      = flag.String("listen", ":9001", "Address to listen on")
	baud      = flag.Int("baud", 115200, "Baud rate")
	port      = flag.String("port", "/dev/ttyUSB0", "Serial port to open")
	stateFile = flag.String("state_file", ".mysensors-state", "File to save/read state")
)

var p *serial.Port

func main() {
	flag.Parse()

	var err error

	// Open serial port.
	c := &serial.Config{Name: *port, Baud: *baud}
	p, err = serial.OpenPort(c)
	if err != nil {
		log.Fatalf("Error opening serial port %s: %v", *port, err)
	}

	// Start MQTT client to send sensor data.
	//mqttCh := make(chan *mysensors.Message)
	//mqtt := &mysensors.MQTTClient{}
	//if err := mqtt.Start(mqttCh); err != nil {
	//		log.Fatalf("Error starting MQTT client: %v", err)
	//}

	// Initialise a new network handler.
	ch := make(chan *mysensors.Message)
	net := mysensors.NewNetwork()
	if err = net.LoadJson(*stateFile); err != nil {
		log.Fatalf("Error loading state: %v", err)
	}
	h := mysensors.NewHandler(p, p, ch, net)

	// Start the web server (for serving prometheus metrics)
	go func() {
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			io.WriteString(w,
				`<!doctype html>
			  <title>MySensors Prometheus Exporter</title>
			  <h1>MySensors Prometheus Exporter</h1>
			  <a href="/metrics">Metrics</a>`)
		})
		http.Handle("/metrics", prometheus.Handler())
		if err := http.ListenAndServe(*addr, nil); err != nil {
			panic(err)
		}
	}()

	// Catch SIGINT and save state before exiting.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		for _ = range sigCh {
			if err = net.SaveJson(*stateFile); err != nil {
				log.Printf("Error writing state file [%s]: %v", *stateFile, err)
			}
			os.Exit(0)
		}
	}()

	// Periodically print sensor status to stdout.
	go func() {
		for _ = range time.Tick(30 * time.Second) {
			net.StatusString()
		}
	}()

	// Start serial handler and pass messages to the Network.
	go h.Start()
	for m := range ch {
		if err := net.HandleMessage(m, h.Tx); err != nil {
			log.Printf("HandleMessage: %v\n", err)
		}
	}
}
