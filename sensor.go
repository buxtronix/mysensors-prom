// This file contains per-sensor routines.
package mysensors

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	// FirstNodeID is the first ID to assign to nodes.
	FirstNodeID = 1
	// GatewayID is the Gateway's ID.
	GatewayID = 0
	// NoChild is the placeholder used for non-sensor node messages.
	NoChild = 255
)

// GaugeMap maps MySensor variables to prometheus variable names.
var GaugeMap = map[SubTypeSetReq]string{
	V_TEMP:        "temperature",
	V_HUM:         "humidity",
	V_PRESSURE:    "pressure",
	V_LEVEL:       "light_level",
	V_LIGHT_LEVEL: "light_percent",
	V_VOLUME:      "volume",
	V_PERCENTAGE:  "battery_level",
	V_VOLTAGE:     "battery_voltage",
}

// CounterMap maps MySensor variables to prometheus variable names.
var CounterMap = map[SubTypeSetReq]string{
	V_VOLUME: "volume",
}

// Gauges contains a mapping from MySensor variables to prometheus gauge objects.
type Gauges struct {
	Gauge  map[SubTypeSetReq]*prometheus.GaugeVec
	Labels []string
}

// Set sets the corresponding gauge to the given value.
func (g *Gauges) Set(t SubTypeSetReq, l []string, v float64) {
	gs, ok := GaugeMap[t]
	if !ok {
		return
	}
	ga, ok := g.Gauge[t]
	if !ok {
		ga = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name:        gs,
				Help:        fmt.Sprintf("MYSENSORS %s", t),
				ConstLabels: prometheus.Labels{"instance": "192.168.0.10:9001"},
			},
			g.Labels,
		)
		prometheus.MustRegister(ga)
		if len(g.Gauge) == 0 {
			g.Gauge = make(map[SubTypeSetReq]*prometheus.GaugeVec)
		}
		g.Gauge[t] = ga
	}
	ga.WithLabelValues(l...).Set(v)
}

// Counters contains a mapping from MySensor variables to prometheus counter objects.
type Counters struct {
	Counter map[SubTypeSetReq]*prometheus.CounterVec
	Labels  []string
}

// Set sets the corresponding counter to the given value.
func (c *Counters) Set(t SubTypeSetReq, l []string, v float64) {
	gs, ok := CounterMap[t]
	if !ok {
		return
	}
	ga, ok := c.Counter[t]
	if !ok {
		ga = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name:        gs,
				Help:        fmt.Sprintf("MYSENSORS %s", t),
				ConstLabels: prometheus.Labels{"instance": "192.168.0.10:9001"},
			},
			c.Labels,
		)
		prometheus.MustRegister(ga)
		if len(c.Counter) == 0 {
			c.Counter = make(map[SubTypeSetReq]*prometheus.CounterVec)
		}
		c.Counter[t] = ga
	}
	ga.WithLabelValues(l...).Add(v)
}

// Network is a container for all sensor nodes.
type Network struct {
	Nodes             map[string]*Node
	gauges            *Gauges
	rxNodePacketCount *prometheus.CounterVec
	Tx                chan *Message `json:"-"`
}

// NewNetwork initialises a new Network.
func NewNetwork() *Network {
	n := &Network{}
	n.Nodes = make(map[string]*Node, 0)
	n.gauges = &Gauges{
		Labels: []string{"location", "node", "sensor"},
	}
	n.Tx = make(chan *Message)
	n.rxNodePacketCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mysensors_received_packets",
			Help: "Packets received from sensor nodes",
		},
		[]string{"node", "location"},
	)
	prometheus.MustRegister(n.rxNodePacketCount)
	return n
}

// HandleMessage handles a MySensors message from the gateway.
func (n *Network) HandleMessage(m *Message, tx chan *Message) error {
	if m.NodeID == GatewayID {
		return n.handleMessage(m, tx)
	}
	nID := fmt.Sprintf("%d", m.NodeID)
	nd, ok := n.Nodes[nID]
	if !ok {
		nd = NewNode(n)
		n.Nodes[nID] = nd
	}
	return nd.HandleMessage(m, tx)
}

// handleMessage handles messages for/from the gateway.
func (n *Network) handleMessage(m *Message, tx chan *Message) error {
	log.Printf("GW MSG: %s\n", m)
	return nil
}

// StatusString prints a formatted representation of the network.
func (n *Network) StatusString() string {
	fmt.Printf(">>> status\n\n")
	nodes := []*Node{}
	for _, node := range n.Nodes {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	for _, node := range nodes {
		fmt.Printf("Node %d [%s %s]    Location: %s    Battery: %d%%\n", node.ID, node.SketchName, node.SketchVersion, node.Location, node.Battery)
		sensors := []*Sensor{}
		for _, sensor := range node.Sensors {
			sensors = append(sensors, sensor)
		}
		sort.Slice(sensors, func(i, j int) bool { return sensors[i].ID < sensors[j].ID })
		for _, s := range sensors {
			fmt.Printf(" Sensor %d [%s]: ", s.ID, s.Presentation)
			vars := []*Var{}
			for _, v := range s.Vars {
				vars = append(vars, v)
			}
			sort.Slice(vars, func(i, j int) bool { return vars[i].Name < vars[j].Name })
			for _, v := range vars {
				fmt.Printf(" %s: %s   ", v.SubType.String(), v.String())
			}
			fmt.Println()
		}
		fmt.Println()
	}
	fmt.Printf("<<< status\n")
	return ""
}

// LoadJson reads a Network from a JSON file.
func (n *Network) LoadJson(f string) error {
	if _, err := os.Stat(f); os.IsNotExist(err) {
		log.Printf("Warning: State file (%s) does not exist, starting anew", f)
		return nil
	}
	data, err := ioutil.ReadFile(f)
	if err != nil {
		return err
	}
	if err = json.Unmarshal(data, n); err != nil {
		return err
	}
	// Re-add parent struct params which arent there after
	// JSON import.
	for _, node := range n.Nodes {
		node.network = n
		for _, s := range node.Sensors {
			s.node = node
		}
	}
	return nil
}

// SaveJson saves the network to a file in Json format.
func (n *Network) SaveJson(f string) error {
	data, err := json.Marshal(n)
	if err != nil {
		return err
	}
	var out bytes.Buffer
	json.Indent(&out, data, "", "  ")
	if err = ioutil.WriteFile(f, out.Bytes(), os.ModePerm); err != nil {
		return err
	}
	return nil
}

// NextNodeID allocates and returns a node ID.
func (n *Network) NextNodeID() uint8 {
	nextID := uint8(FirstNodeID)
	for _, node := range n.Nodes {
		if node.ID >= nextID {
			nextID = node.ID + 1
		}
	}
	return nextID
}

// Node is a node that may contain multiple sensors.
type Node struct {
	// ID is the node ID.
	ID uint8
	// Battery is the battery level percent.
	Battery int64
	// Location per the configuration.
	Location string
	// Version as reported.
	Version string
	// Sketch name.
	SketchName string
	// SketchVersion.
	SketchVersion string
	// Sensors are all sensors attached to the node.
	Sensors map[string]*Sensor
	// network is the parent network.
	network *Network
}

func NewNode(ne *Network) *Node {
	n := &Node{network: ne}
	n.Sensors = make(map[string]*Sensor)
	return n
}

func (n *Node) HandleMessage(m *Message, tx chan *Message) error {
	n.ID = m.NodeID
	n.network.rxNodePacketCount.WithLabelValues(strconv.Itoa(int(n.ID)), n.Location).Inc()
	sID := fmt.Sprintf("%d", m.ChildSensorID)
	if m.ChildSensorID == NoChild {
		return n.handleMessage(m, tx)
	}
	cs, ok := n.Sensors[sID]
	if !ok {
		cs = NewSensor(n)
		n.Sensors[sID] = cs
	}
	return cs.HandleMessage(m, tx)
}

func (n *Node) handleMessage(m *Message, tx chan *Message) error {
	if m.Type != MsgInternal {
		return fmt.Errorf("Unknown message to child id %d", NoChild)
	}
	subType := m.SubType.(SubTypeInternal)
	switch subType {
	case I_BATTERY_LEVEL:
		n.Battery, _ = strconv.ParseInt(string(m.Payload), 10, 32)
		n.network.gauges.Set(V_PERCENTAGE, []string{n.Location, strconv.Itoa(int(n.ID)), "0"}, float64(n.Battery)/100.0)
	case I_VERSION:
		n.Version = string(m.Payload)
	case I_SKETCH_NAME:
		n.SketchName = string(m.Payload)
	case I_SKETCH_VERSION:
		n.SketchVersion = string(m.Payload)
	default:
		log.Printf("UNKN: %s\n", m.String())
	}
	return nil
}

// Sensor is a child sensor.
type Sensor struct {
	// ID is the sensor ID.
	ID uint8
	// Presentation is the sensor subtype presented.
	Presentation SubTypePresentation
	// Vars are the variables presented by this child sensor.
	Vars map[string]*Var
	// Node is the parent node.
	node *Node
}

func NewSensor(n *Node) *Sensor {
	s := &Sensor{node: n}
	s.Vars = make(map[string]*Var, 0)
	return s
}

func (s *Sensor) HandleMessage(m *Message, tx chan *Message) error {
	s.ID = m.ChildSensorID
	switch m.Type {
	case MsgPresentation:
		s.Presentation = m.SubType.(SubTypePresentation)
		log.Printf("PRES: %s\n", m)
	case MsgSet:
		subType := m.SubType.(SubTypeSetReq)
		if s.Vars == nil {
			s.Vars = make(map[string]*Var, 0)
		}
		if _, ok := s.Vars[subType.String()]; !ok {
			switch subType {
			case V_TEMP, V_HUM, V_PRESSURE, V_LEVEL, V_VOLUME, V_VOLTAGE, V_LIGHT_LEVEL:
				s.Vars[subType.String()] = &Var{Type: varFloat}
			default:
				s.Vars[subType.String()] = &Var{Type: varString}
			}
		}
		s.Vars[subType.String()].SubType = subType
		s.Vars[subType.String()].Set(string(m.Payload))
		if s.Vars[subType.String()].Type == varFloat {
			s.node.network.gauges.Set(subType, []string{s.node.Location, strconv.Itoa(int(s.node.ID)), strconv.Itoa(int(s.ID))}, s.Vars[subType.String()].FloatVal)
		}
		log.Printf("SET: %s\n", m)
	case MsgReq:
		subType := m.SubType.(SubTypeSetReq)
		vr := "0"
		if val, ok := s.Vars[subType.String()]; ok {
			vr = val.Value()
		}
		r := m.Copy()
		r.SubType = subType
		r.Payload = []byte(vr)
		tx <- r
		log.Printf("REQ: %s\n", m)
	}
	return nil
}

const (
	varString = "string"
	varFloat  = "float"
)

type Var struct {
	Name      string
	Type      string
	SubType   SubTypeSetReq
	FloatVal  float64
	StringVal string
}

func (v *Var) Set(val string) error {
	switch v.Type {
	case varString:
		v.StringVal = val
	case varFloat:
		fv, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return err
		}
		v.FloatVal = fv
	}
	return nil
}

func (v *Var) Value() string {
	switch v.Type {
	case varString:
		return v.StringVal
	case varFloat:
		return strconv.FormatFloat(v.FloatVal, 'f', 2, 64)
	}
	return ""
}

func (v *Var) String() string {
	return v.Value()
}
