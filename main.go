package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/OpenSlides/openslides-autoupdate-service/pkg/auth"
	"github.com/OpenSlides/openslides-autoupdate-service/pkg/datastore"
	"github.com/OpenSlides/openslides-autoupdate-service/pkg/environment"
	messageBusRedis "github.com/OpenSlides/openslides-autoupdate-service/pkg/redis"
	"github.com/OpenSlides/openslides-icc-service/internal/applause"
	"github.com/OpenSlides/openslides-icc-service/internal/icchttp"
	"github.com/OpenSlides/openslides-icc-service/internal/icclog"
	"github.com/OpenSlides/openslides-icc-service/internal/notify"
	"github.com/OpenSlides/openslides-icc-service/internal/redis"
	"github.com/alecthomas/kong"
)

//go:generate  sh -c "go run main.go build-doc > environment.md"

var (
	envICCServicePort = environment.NewVariable("ICC_PORT", "9007", "Port on which the service listen on.")
	envICCRedisHost   = environment.NewVariable("ICC_REDIS_HOST", "localhost", "The host of the redis instance to save icc messages.")
	envICCRedisPort   = environment.NewVariable("ICC_REDIS_PORT", "6379", "The port of the redis instance to save icc messages.")
	envICCRedisDSN    = environment.NewVariable("ICC_REDIS_DSN", "", "Redis connection URL")
)

var cli struct {
	Run      struct{} `cmd:"" help:"Runs the service." default:"withargs"`
	BuildDoc struct{} `cmd:"" help:"Build the environment documentation."`
	Health   struct {
		Host     string `help:"Host of the service" short:"h" default:"localhost"`
		Port     string `help:"Port of the service" short:"p" default:"9007" env:"ICC_PORT"`
		UseHTTPS bool   `help:"Use https to connect to the service" short:"s"`
		Insecure bool   `help:"Accept invalid cert" short:"k"`
	} `cmd:"" help:"Runs a health check."`
}

func main() {
	ctx, cancel := environment.InterruptContext()
	defer cancel()

	icclog.SetInfoLogger(log.Default())

	kongCTX := kong.Parse(&cli, kong.UsageOnError())
	switch kongCTX.Command() {
	case "run":
		if err := contextDone(run(ctx)); err != nil {
			handleError(err)
			os.Exit(1)
		}

	case "build-doc":
		if err := contextDone(buildDocu()); err != nil {
			handleError(err)
			os.Exit(1)
		}

	case "health":
		if err := contextDone(icchttp.HealthClient(ctx, cli.Health.UseHTTPS, cli.Health.Host, cli.Health.Port, cli.Health.Insecure)); err != nil {
			handleError(err)
			os.Exit(1)
		}
	}
}

func run(ctx context.Context) error {
	lookup := new(environment.ForProduction)

	service, err := initService(lookup)
	if err != nil {
		return fmt.Errorf("init services: %w", err)
	}

	return service(ctx)
}

func buildDocu() error {
	lookup := new(environment.ForDocu)

	if _, err := initService(lookup); err != nil {
		return fmt.Errorf("init services: %w", err)
	}

	doc, err := lookup.BuildDoc()
	if err != nil {
		return fmt.Errorf("build doc: %w", err)
	}

	fmt.Println(doc)
	return nil
}

// initService initializes all packages needed for the icc service.
//
// Returns a the service as callable.
func initService(lookup environment.Environmenter) (func(context.Context) error, error) {
	var backgroundTasks []func(context.Context, func(error))
	listenAddr := ":" + envICCServicePort.Value(lookup)

	// Redis as message bus for datastore and logout events.
	messageBus := messageBusRedis.New(lookup)

	// Datastore Service.
	datastoreService, dsBackground, err := datastore.New(lookup, messageBus)
	if err != nil {
		return nil, fmt.Errorf("init datastore: %w", err)
	}
	backgroundTasks = append(backgroundTasks, dsBackground)

	// Auth Service.
	authService, authBackground := auth.New(lookup, messageBus)
	backgroundTasks = append(backgroundTasks, authBackground)

	backend := initRedis(lookup)

	notifyService, notifyBackground := notify.New(backend)
	backgroundTasks = append(backgroundTasks, notifyBackground)

	applauseService, applauseBackground := applause.New(backend, datastoreService)
	backgroundTasks = append(backgroundTasks, applauseBackground)

	service := func(ctx context.Context) error {
		for _, bg := range backgroundTasks {
			go bg(ctx, handleError)
		}

		// Start http server.
		fmt.Printf("Listen on %s\n", listenAddr)
		return Run(ctx, listenAddr, notifyService, applauseService, authService)
	}

	return service, nil
}

func initRedis(lookup environment.Environmenter) *redis.Redis {
	if envICCRedisDSN.Value(lookup) != "" {
		return redis.NewByURL(envICCRedisDSN.Value(lookup))
	}

	return redis.New(envICCRedisHost.Value(lookup) + ":" + envICCRedisPort.Value(lookup))
}

// Run starts a webserver
func Run(ctx context.Context, addr string, notifyService *notify.Notify, applauseService *applause.Applause, auth icchttp.Authenticater) error {
	mux := http.NewServeMux()
	icchttp.HandleHealth(mux)
	notify.HandleReceive(mux, notifyService, auth)
	notify.HandlePublish(mux, notifyService, auth)
	applause.HandleReceive(mux, applauseService, auth)
	applause.HandleSend(mux, applauseService, auth)

	srv := &http.Server{
		Addr:        addr,
		Handler:     mux,
		BaseContext: func(net.Listener) context.Context { return ctx },
	}

	// Shutdown logic in separate goroutine.
	wait := make(chan error)
	go func() {
		<-ctx.Done()
		if err := srv.Shutdown(context.Background()); err != nil {
			wait <- fmt.Errorf("HTTP server shutdown: %w", err)
			return
		}
		wait <- nil
	}()

	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		return fmt.Errorf("HTTP Server failed: %v", err)
	}

	return <-wait
}

// contextDone returns an empty error if the context is done or exceeded
func contextDone(err error) error {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	return err
}

// handleError handles an error.
//
// Ignores context closed errors.
func handleError(err error) {
	if contextDone(err) == nil {
		return
	}

	icclog.Info("Error: %v", err)
}
