package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/gmallard/stompngo"
	flag "github.com/ogier/pflag"
	"io/ioutil"
	"net"
	"os"
	"strings"
)

var (
	nameString string = "publisher"
	verString  string = "1.0.0"

	help     *bool   = flag.BoolP("help", "h", false, "show help")
	msg      *string = flag.StringP("data", "m", "", "data/message to send")
	protocol *string = flag.StringP("protocol", "p", "tcp", "protocol to use: 'tcp' or 'ssl'")
	format   *string = flag.StringP("format", "o", "default", "message format 'json','xml','csv','keyvalue','sql','unformatted','kv','csv:kv','csv:keyvalue','csv:json'")
	queue    *string = flag.StringP("queue", "q", "", "queue, prepend with /topic/ for topic")
	username *string = flag.String("username", "", "username override")
	password *string = flag.String("password", "", "password override")
	version  *bool   = flag.Bool("version", false, "show version")
	verbose  *bool   = flag.BoolP("verbose", "v", false, "turn on verbose logging")
	debug    *bool   = flag.BoolP("debug", "d", false, "turn on debug logging")
	secure   *bool   = flag.Bool("secure", true, "send as secure")
	broker   *string = flag.StringP("broker", "b", "", "fully.qualified.broker:port")
	msgfile  *string = flag.StringP("file", "f", "", "file name of data to send to ibi")
)

// main
func main() {
	fmt.Println(Version())

	// parse command line
	flag.Parse()

	if *help {
		flag.Usage()
		os.Exit(0)
	}

	if *version {
		Version()
		os.Exit(0)
	}

	// need a broker and a queue
	if *broker == "" || *queue == "" {
		fmt.Println("Need -t and -q")
		flag.Usage()
		os.Exit(1)
	}
	// need messages ...
	if *msg == "" && *msgfile == "" {
		fmt.Println("Need either -m or -f")
		flag.Usage()
		os.Exit(1)
	}
	// ... but only one or the other
	if *msg != "" && *msgfile != "" {
		fmt.Println("Need either -m or -f")
		flag.Usage()
		os.Exit(1)
	}

	var conn Publisher
	// SetLogFile("/tmp/" + nameString + ".out")
	conn.SetMsgHeader(*queue, "me", nameString, verString, *format)

	err := conn.Connect(*protocol, "STOMP", *broker)
	if err != nil {
		fmt.Printf("ERROR: Connect returned error: %+v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Connect succeeded\n")

	if *msg != "" {
		fmt.Printf("Sending message to %+v\n", conn.GetActivePublisher())
		err := conn.Send(*msg)
		if err != nil {
			fmt.Printf("ERROR: Send returned error: %+v\n", err)
			os.Exit(1)
		}
		fmt.Printf("--message: \"%+v\"\n---------------------------------\n", *msg)
	}
	if *msgfile != "" {
		if *msgfile == "-" {
			fmt.Printf("Sending 'stdin' to %+v, (ctrl-D when finished)\n", conn.GetActivePublisher())
			// read from stdin
			stdin := bufio.NewReader(os.Stdin)
			//if *verbose {
			//fmt.Println("(Ctrl-D to end) ")   // need to determine how to tell the difference between piping data
			//}									// in and the user entering data before I can reenable
			for {
				//fmt.Print("(Ctrl-D to end) ")
				line, error := stdin.ReadString('\n')
				if error != nil {
					fmt.Println("")
					break
				}
				if line != "" && line != "\n" {
					line = strings.Replace(line, "\n", "", -1)
					if error = conn.Send(line); error != nil {
						fmt.Printf("ERROR: Send returned error: %+v\n", error)
					}
					if *verbose {
						fmt.Println("--sent: " + line)
					}
				}
			}
			fmt.Printf("---------------------------------\n")
		} else {
			fmt.Printf("Sending file %+v to %+v\n", *msgfile, conn.GetActivePublisher())
			err := conn.SendFile(*msgfile)
			if err != nil {
				fmt.Printf("ERROR: SendFile returned error: %+v\n", err)
				os.Exit(1)
			}
		}
	}

	err = conn.Disconnect()
	if err != nil {
		fmt.Printf("ERROR: Disconnect returned error: %+v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Disconnect succeeded\n")
}

func Version() (version string) {
	return nameString + " " + verString
}

type Publisher struct {
	transportConn     net.Conn
	transportProtocol string //tcp or ssl or ...
	messageProtocol   string // STOMP or ...
	stompConnHeader   stompngo.Headers
	// pub
	stompConn         *stompngo.Connection
	stompMsgHeader    stompngo.Headers
	stompMsgHeaderSet bool
	connected         bool // true if connected to a feeder
	// message cache
	messageCache [2]string
	MBSTarget    string
}

// isConnected return true if connected to a message bus broker, false otherwise.
//
//   if pub.isConnected()
func (p *Publisher) isConnected() bool {
	return p.connected
}

// Connect makes a transport protocol (tcp) connection, then a message protocol (STOMP) connection to the
// message bus (activeMQ) on the broker, returning nil if successful, else an error.
//
//   err = pub.Connect("tcp", "STOMP", "host:3333")
//
func (p *Publisher) Connect(tpProtocol, msgProtocol, feederport string) (e error) {
	p.transportProtocol = tpProtocol
	p.messageProtocol = msgProtocol

	fmt.Println("INFO: Connecting ... ")
	p.transportConn, e = net.Dial(p.transportProtocol, feederport)
	if e != nil {
		fmt.Println("INFO: failed to connect to " + feederport + " " + fmt.Sprintf("%v", e))
		// continue
	} else {
		fmt.Println("INFO: ... connected to " + feederport)
		// do the STOMP connection here
		switch strings.ToLower(p.messageProtocol) {
		case "stomp":
			p.stompConnHeader = stompngo.Headers{
				"host", feederport,
				"accept-version", "1.1"}
			if *username != "" {
				p.stompConnHeader = append(p.stompConnHeader, "login", *username)
				p.stompConnHeader = append(p.stompConnHeader, "passcode", *password)
			}
			fmt.Println("INFO: STOMP Header: " + fmt.Sprintf("%v\n", p.stompConnHeader))
			p.stompConn, e = stompngo.Connect(p.transportConn, p.stompConnHeader)
			if e != nil {
				return errors.New("ERROR: Connect: failed to connect [" + p.transportProtocol + "][" + p.messageProtocol + "] on host: " + feederport + ", with ERROR: " + fmt.Sprintf("%v", e))
			}
		case "ampq":
			fallthrough
		case "openwire":
			fallthrough
		default:
			return errors.New("ERROR: Connect: message protocol [" + p.messageProtocol + "]: not supported")
		}
		p.connected = true
		return nil
	}
	return nil
}

// Disconnect closes the messageProtocal connection then the transportProtocol connection
//
//   error := pub.Disconnect()
func (p *Publisher) Disconnect() (e error) {
	if p.isConnected() {
		switch strings.ToLower(p.messageProtocol) {
		case "stomp":
			e = p.stompConn.Disconnect(p.stompConnHeader)
			if e != nil {
				return errors.New("ERROR: Disconnect: disconnect [" + p.messageProtocol + "] failed with " + fmt.Sprintf("%v", e))
			}
			e = p.transportConn.Close()
			if e != nil {
				return errors.New("ERROR: Disconnect: close [" + p.transportProtocol + "] failed with " + fmt.Sprintf("%v", e))
			}
		default:

		}
		p.connected = false
		return nil
	}
	return errors.New("ERROR: Disconnect: disconnect attempt when not connected")
}

// SetMsgHeader sets up the STOMP message header for publishing
//
//   pub.SetMsgHeader("TEST", "joebob", "myapp", "1.0.2", "json")
func (p *Publisher) SetMsgHeader(to, from, via, version, format string) {
	p.stompMsgHeader = append(p.stompMsgHeader, "destination", to)
	p.stompMsgHeader = append(p.stompMsgHeader, "username", from)
	p.stompMsgHeader = append(p.stompMsgHeader, "version", via+" "+version)
	p.stompMsgHeader = append(p.stompMsgHeader, "format", format)
	p.stompMsgHeader = append(p.stompMsgHeader, "persistent", "true")
	p.stompMsgHeader = append(p.stompMsgHeader, "priority", "5")

	fqhn, err := os.Hostname()
	if err != nil {
		fqhn = "unknown"
	}
	p.stompMsgHeader = append(p.stompMsgHeader, "hostname", fqhn)
	p.stompMsgHeaderSet = true
}

// Send sends the message to the previously connected feeder
//
//   error := pub.Send(data)
func (p *Publisher) Send(message string) (e error) {
TOP:
	if p.isConnected() {
		if !p.stompMsgHeaderSet {
			return errors.New("ERROR: Send: send attempt to broker with header not set.")
		} else {
			p.messageCache[1] = p.messageCache[0]
			p.messageCache[0] = message
			e = p.stompConn.Send(p.stompMsgHeader, message)
			if e == nil {
				return nil
			} else {
				// Send failed, attempt to reconnect
				fmt.Println("WARN: failed to send: " + p.messageCache[1])
				fmt.Println("WARN: send error: " + fmt.Sprintf("%v", e))
				e = p.Disconnect()
				p.connected = false
				// time.Sleep(10 * time.Second)
				fmt.Println("INFO: Attempting reconnect ...")
				e = p.Connect(p.transportProtocol, p.messageProtocol, p.MBSTarget)

				// time.Sleep(10 * time.Second)
				// resend the previous message
				fmt.Println("INFO: resending: " + p.messageCache[1])
				e = p.stompConn.Send(p.stompMsgHeader, p.messageCache[1])
				goto TOP
			}
		}
	}
	return errors.New("ERROR: Send: send attempt to broker when not connected.")
}

// SendFile opens the file and sends each non-blank line to the connected feeder
//
//   error := pub.SendFile("./messages.txt")
func (p *Publisher) SendFile(pathToFile string, debug ...bool) (e error) {
	// a real file
	rawBytes, error := ioutil.ReadFile(pathToFile) // no close required
	if error != nil {
		return errors.New("ERROR: SendFile: ioutil.ReadFile failed: " + fmt.Sprintf("%v", error))
	}
	text := string(rawBytes)
	lines := strings.Split(text, "\n")
	for _, line := range lines {
		if line != "" {
			if len(debug) > 0 && debug[0] {
				fmt.Println(" -sending: " + line)
			}
			error = p.Send(line)
			if error != nil {
				return errors.New("ERROR: SendFile: send failed: " + fmt.Sprintf("%v", error))
			}
		}
	}
	return nil
}

// GetActivePublisher returns the feeder:port being published to
func (p *Publisher) GetActivePublisher() string {
	return p.MBSTarget
}
