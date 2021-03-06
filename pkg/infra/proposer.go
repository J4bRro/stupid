package infra

import (
	"context"
	"io"
//	"fmt"
	"time"
	"sync/atomic"
	"github.com/hyperledger/fabric-protos-go/common"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric-protos-go/peer"
	log "github.com/sirupsen/logrus"
)

type Proposers struct {
	workers [][]*Proposer
	//one proposer per connection per peer
	client int
	logger *log.Logger
}

var Count1 int64
var Count2 int64

func CreateProposers(conn, client int, nodes []Node, logger *log.Logger) *Proposers {
	var ps [][]*Proposer
	//one proposer per connection per peer
	for _, node := range nodes {
		row := make([]*Proposer, conn)
		TLSCACert, err := GetTLSCACerts(node.TLSCACert)
		if err != nil {
			panic(err)
		}
		for j := 0; j < conn; j++ {
			row[j] = CreateProposer(node.Addr, TLSCACert, logger)
		}
		ps = append(ps, row)
	}

	return &Proposers{workers: ps, client: client, logger: logger}
}

func (ps *Proposers) Start(signed []chan *Elements, processed chan *Elements, done <-chan struct{}, config Config) {
	ps.logger.Infof("Start sending transactions.")
	for i := 0; i < len(config.Endorsers); i++ {
		for j := 0; j < config.NumOfConn; j++ {
			go ps.workers[i][j].Start(signed[i], processed, done, len(config.Endorsers))
		}
	}
}

type Proposer struct {
	e      peer.EndorserClient
	Addr   string
	logger *log.Logger
}

func CreateProposer(addr string, TLSCACert []byte, logger *log.Logger) *Proposer {
	endorser, err := CreateEndorserClient(addr, TLSCACert)
	if err != nil {
		panic(err)
	}

	return &Proposer{e: endorser, Addr: addr, logger: logger}
}

func (p *Proposer) Start(signed, processed chan *Elements, done <-chan struct{}, threshold int) {
	for {
		select {
		case s := <-signed:
			atomic.AddInt64(&Count1, 1)
			//send sign proposal to peer for endorsement
			r, err := p.e.ProcessProposal(context.Background(), s.SignedProp)
			if err != nil || r.Response.Status < 200 || r.Response.Status >= 400 {
				if r == nil {
					p.logger.Errorf("Err processing proposal: %s, status: unknown, addr: %s \n", err, p.Addr)
				} else {
					p.logger.Errorf("Err processing proposal: %s, status: %d, addr: %s \n", err, r.Response.Status, p.Addr)
				}
				continue
			}
			s.lock.Lock()
			//collect for endorsement
			s.Responses = append(s.Responses, r)
			if len(s.Responses) >= threshold {
				processed <- s
			}
			s.lock.Unlock()
		case <-done:
			return
		}
	}
}

type Broadcasters []*Broadcaster

func CreateBroadcasters(conn int, orderer Node, logger *log.Logger) Broadcasters {
	bs := make(Broadcasters, conn)
	TLSCACert, err := GetTLSCACerts(orderer.TLSCACert)
	if err != nil {
		panic(err)
	}
	for i := 0; i < conn; i++ {
		bs[i] = CreateBroadcaster(orderer.Addr, TLSCACert, logger)
	}

	return bs
}

func (bs Broadcasters) Start(envs <-chan *Elements, done <-chan struct{}) {
	for _, b := range bs {
		go b.StartDraining()
		go b.Start(envs, done)
	}
}

type Broadcaster struct {
	c      orderer.AtomicBroadcast_BroadcastClient
	logger *log.Logger
}

func CreateBroadcaster(addr string, tlscacert []byte, logger *log.Logger) *Broadcaster {
	client, err := CreateBroadcastClient(addr, tlscacert)
	if err != nil {
		panic(err)
	}

	return &Broadcaster{c: client, logger: logger}
}

func (b *Broadcaster) Start(envs <-chan *Elements, done <-chan struct{}) {
	b.logger.Debugf("Start sending broadcast")
	for {
		select {
		case e := <-envs:
			t1 := time.Now()
			atomic.AddInt64(&Count2, 1)
			err := b.c.Send(e.Envelope)
			if err != nil {
				b.logger.Errorf("Failed to broadcast env: %s\n", err)
			}
			time.Now().Sub(t1).Milliseconds()
			
			//if subTime > 10 {
				//fmt.Println(subTime)
			//}
			

		case <-done:
			return
		}
	}
}

func (b *Broadcaster) StartDraining() {
	for {
		res, err := b.c.Recv()
		if err != nil {
			if err == io.EOF {
				return
			}
			b.logger.Errorf("Recv broadcast err: %s, status: %+v\n", err, res)
			panic("bcast recv err")
		}

		if res.Status != common.Status_SUCCESS {
			b.logger.Errorf("Recv errouneous status: %s\n", res.Status)
			panic("bcast recv err")
		}

	}
}
