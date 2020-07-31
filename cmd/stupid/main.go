package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
	"flag"
	"sync/atomic"
	"github.com/guoger/stupid/pkg/infra"
	log "github.com/sirupsen/logrus"
)

const loglevel = "STUPID_LOGLEVEL"

func main() {
	var flagValue int
	flag.IntVar(&flagValue, "count", 10, "help message for flagname")
	flag.Parse()
	logger := log.New()
	logger.SetLevel(log.WarnLevel)
	if customerLevel, customerSet := os.LookupEnv(loglevel); customerSet {
		if lvl, err := log.ParseLevel(customerLevel); err == nil {
			logger.SetLevel(lvl)
		}
	}
	//if len(os.Args) != 3 {
	//	fmt.Printf("Usage: stupid config.yaml 500\n")
	//	os.Exit(1)
	//}
	config := infra.LoadConfig(os.Args[1])
	N, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}
	crypto := config.LoadCrypto()

	raw := make(chan *infra.Elements, 100)
	signed := make([]chan *infra.Elements, len(config.Endorsers))
	processed := make(chan *infra.Elements, 100)
	envs := make(chan *infra.Elements, 100)
	done := make(chan struct{})

	assember := &infra.Assembler{Signer: crypto}

	for i := 0; i < len(config.Endorsers); i++ {
		signed[i] = make(chan *infra.Elements, 100)
	}
	var num int64
	go func() {
		t:=time.NewTicker(5*time.Second)
		for {
			<-t.C
			fmt.Println(len(raw), len(signed[0]), len(processed), len(envs), atomic.LoadInt64(&num))
			fmt.Println(infra.Count1, infra.Count2)
		}
	}()

	for i := 0; i < flagValue; i++ {
		go assember.StartSigner(raw, signed, done)
		go assember.StartIntegrator(processed, envs, done)
	}

	proposor := infra.CreateProposers(config.NumOfConn, config.ClientPerConn, config.Endorsers, logger)
	proposor.Start(signed, processed, done, config)

	broadcaster := infra.CreateBroadcasters(config.NumOfConn, config.Orderer, logger)
	broadcaster.Start(envs, done)

	observer := infra.CreateObserver(config.Channel, config.Committer, crypto, logger)

	start := time.Now()
	go observer.Start(N, start)
	for j := 0; j < 10; j++ {
	   go func(){
			for i := 0; i < N/10; i++ {
				prop := infra.CreateProposal(
					crypto,
					config.Channel,
					config.Chaincode,
					config.Version,
					config.Args...,
				)
				raw <- &infra.Elements{Proposal: prop}
				atomic.AddInt64(&num, 1)
			}
		}()
	}

	observer.Wait()
	duration := time.Since(start)
	close(done)
	logger.Infof("Completed processing transactions.")
	fmt.Printf("tx: %d, duration: %+v, tps: %f\n", N, duration, float64(N)/duration.Seconds())
	os.Exit(0)
}
