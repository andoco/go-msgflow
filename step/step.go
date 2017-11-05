package step

import log "github.com/sirupsen/logrus"

// Step is an interface for a node in the processing flow.
type Step interface {
	// Start will create a goroutine for performing the step activities.
	Start()

	// ConnectTo will setup a channel for sending output from the current
	// node to the input of the other node.
	ConnectTo(other Step)
}

// F is the function type that performs the activity for a step
// based on an input value, and producing an output value.
type F func(in interface{}) (interface{}, error)

// C is the channel type used for receiving input, and sending output.
type C chan interface{}

// step is a struct for a node in the processing flow.
type step struct {
	id  string
	fn  F
	in  C
	out C
}

func (s step) Start() {
	log.WithField("id", s.id).Debug("Starting step")

	go func() {
		for {
			var inData interface{}
			if s.in != nil {
				log.WithField("id", s.id).Debug("Waiting for input")
				inData = <-s.in
				log.WithField("id", s.id).WithField("input", inData).Debug("Received input")
			}

			log.WithField("id", s.id).WithField("input", inData).Debug("Handling input")
			outData, err := s.fn(inData)
			if err != nil {
				log.WithError(err).Error("error while executing step")
				continue
			}

			log.WithField("id", s.id).WithField("output", outData).Debug("Will be sending output data")
			if s.out != nil && outData != nil {
				log.WithField("id", s.id).WithField("output", outData).Debug("Sending output data")
				s.out <- outData
			}

			inData = nil
		}
	}()
}

func (s *step) ConnectTo(other Step) {
	s.out = make(C)
	os := other.(*step)
	os.in = s.out
}

// New creates a new activity step identifies by id.
func New(id string, fn F) *step {
	return &step{
		id: id,
		fn: fn,
	}
}
