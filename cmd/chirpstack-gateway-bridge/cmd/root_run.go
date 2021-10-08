package cmd

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/brocaar/chirpstack-gateway-bridge/internal/backend"
	"github.com/brocaar/chirpstack-gateway-bridge/internal/commands"
	"github.com/brocaar/chirpstack-gateway-bridge/internal/config"
	"github.com/brocaar/chirpstack-gateway-bridge/internal/filters"
	"github.com/brocaar/chirpstack-gateway-bridge/internal/forwarder"
	"github.com/brocaar/chirpstack-gateway-bridge/internal/integration"
	"github.com/brocaar/chirpstack-gateway-bridge/internal/metadata"
	"github.com/brocaar/chirpstack-gateway-bridge/internal/metrics"
)

/*
三类command
1：down - downlink transmission
Request the gateway to schedule a downlink transmission.
The items must contain at least one downlink option but can contain multiple items.


2：exec - Command execution request
This will request the execution of a command by the ChirpStack Gateway Bridge.
Please note that these commands must be pre-configured in the Configuration file.

3：raw - Raw packet-forwarder command
This payload is used for raw packet-forwarder commands
that are not integrated with the ChirpStack Gateway Bridge
参考: https://www.chirpstack.io/gateway-bridge/payloads/commands/

关于Events
参考: https://www.chirpstack.io/gateway-bridge/payloads/events/

关于states
参考:https://www.chirpstack.io/gateway-bridge/payloads/states/



*/
func run(cmd *cobra.Command, args []string) error {

	tasks := []func() error{
		setLogLevel,
		setSyslog,
		printStartMessage,
		setupFilters,     //设置NetId和JoinEUI过滤器。JoinEUI过滤器会过滤EUI的join请求
		setupBackend,     // 创建一个backend服务
		setupIntegration, //创建一个mqtt服务
		setupForwarder,   // 给backend服务和mqtt服务设置 forwarder
		setupMetrics,     // 配置访问Prometheus监控的量度指标
		setupMetaData,
		setupCommands,    // 设置命令执行函数。命令是下发给硬件的
		startIntegration, // 对我们来讲就是启动MQTT服务
		startBackend,     // 启动backend服务，接受lora设备数据的服务
	}

	for _, t := range tasks {
		if err := t(); err != nil {
			log.Fatal(err)
		}
	}

	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	log.WithField("signal", <-sigChan).Info("signal received")
	log.Warning("shutting down server")

	integration.GetIntegration().Stop()

	return nil
}

func setLogLevel() error {
	log.SetLevel(log.Level(uint8(config.C.General.LogLevel)))
	return nil
}

func printStartMessage() error {
	log.WithFields(log.Fields{
		"version": version,
		"docs":    "https://www.chirpstack.io/gateway-bridge/",
	}).Info("starting ChirpStack Gateway Bridge")
	return nil
}

func setupBackend() error {
	if err := backend.Setup(config.C); err != nil {
		return errors.Wrap(err, "setup backend error")
	}
	return nil
}

func setupIntegration() error {
	if err := integration.Setup(config.C); err != nil {
		return errors.Wrap(err, "setup integration error")
	}
	return nil
}

func setupForwarder() error {
	if err := forwarder.Setup(config.C); err != nil {
		return errors.Wrap(err, "setup forwarder error")
	}
	return nil
}

func setupMetrics() error {
	if err := metrics.Setup(config.C); err != nil {
		return errors.Wrap(err, "setup metrics error")
	}
	return nil
}

func setupMetaData() error {
	if err := metadata.Setup(config.C); err != nil {
		return errors.Wrap(err, "setup meta-data error")
	}
	return nil
}

func setupFilters() error {
	if err := filters.Setup(config.C); err != nil {
		return errors.Wrap(err, "setup filters error")
	}
	return nil
}

func setupCommands() error {
	if err := commands.Setup(config.C); err != nil {
		return errors.Wrap(err, "setup commands error")
	}
	return nil
}

func startIntegration() error {
	if err := integration.GetIntegration().Start(); err != nil {
		return errors.Wrap(err, "start integration error")
	}
	return nil
}

func startBackend() error {
	if err := backend.GetBackend().Start(); err != nil {
		return errors.Wrap(err, "start backend error")
	}
	return nil
}
