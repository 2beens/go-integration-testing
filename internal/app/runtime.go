package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/2beens/simple-go-service/internal/api"
	"github.com/2beens/simple-go-service/internal/db"
	"github.com/2beens/simple-go-service/internal/db/repo"
	"github.com/2beens/simple-go-service/internal/kafka"
	"github.com/2beens/simple-go-service/internal/outbound"
	"github.com/2beens/simple-go-service/internal/payment"
	redisclient "github.com/2beens/simple-go-service/internal/redis"
)

// Runtime owns the running app and all resources that must be shut down together.
type Runtime struct {
	BaseURL string

	pgStore       *db.Postgres
	redisClient   *redisclient.Client
	kafkaProducer *kafka.Producer
	kafkaConsumer *kafka.Consumer

	httpServer *http.Server
	serverErr  chan error

	runCancel    context.CancelFunc
	consumerDone chan struct{}

	shutdownOnce sync.Once
	shutdownErr  error
}

// Start initializes dependencies, starts the HTTP server, and starts the Kafka consumer loop.
func Start(ctx context.Context, log *slog.Logger) (*Runtime, error) {
	// Load config from environment variables.
	// Note: we can change the config keys to simulate misconfiguration and test that the integration tests catch it,
	// while unit tests would not.
	cfg := loadConfigFromEnv()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	if log == nil {
		log = slog.Default()
	}

	log.Debug("config loaded", "values", cfg)

	if len(cfg.KafkaBrokers) == 0 {
		return nil, errors.New("kafka brokers are required")
	}

	runtime := &Runtime{}

	pgStore, err := db.New(ctx, cfg.PostgresDSN)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	runtime.pgStore = pgStore

	// Note: here we can simulate a bug in the app bootstrap by commenting out the migrations.
	// Unit tests would still pass, but the app would be broken.
	// That's why we have integration tests to catch these kinds of bugs.
	if err := pgStore.RunMigrations(ctx); err != nil {
		return runtime, fmt.Errorf("run migrations: %w", err)
	}

	redisClient, err := redisclient.New(ctx, redisclient.Config{Addr: cfg.RedisAddr})
	if err != nil {
		return runtime, fmt.Errorf("connect redis: %w", err)
	}
	runtime.redisClient = redisClient

	kafkaProducer := kafka.NewProducer(cfg.KafkaBrokers)
	runtime.kafkaProducer = kafkaProducer

	kafkaConsumer := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: cfg.KafkaBrokers,
		GroupID: kafka.ConsumerGroupID,
		Topic:   kafka.OutboundPaymentsTopic,
	})
	runtime.kafkaConsumer = kafkaConsumer

	httpClient := &http.Client{Timeout: 10 * time.Second}
	form3Client := payment.New(cfg.Form3BaseURL, httpClient)
	paymentsRepo := repo.NewPaymentsRepo(pgStore.Pool())
	svc := outbound.NewService(paymentsRepo, redisClient, form3Client, kafkaProducer)
	handler := api.NewHandler(svc)
	router := api.NewRouter(handler)

	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return runtime, fmt.Errorf("listen on %q: %w", cfg.HTTPAddr, err)
	}
	runtime.BaseURL = baseURLFromListener(listener)

	httpServer := &http.Server{
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	runtime.httpServer = httpServer
	serverErr := make(chan error, 1)
	runtime.serverErr = serverErr
	go func() {
		log.Info("starting HTTP server", "addr", listener.Addr().String())
		if err := httpServer.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
			select {
			case serverErr <- fmt.Errorf("http server: %w", err):
			default:
			}
		}
	}()

	// Note: here too, we can simulate a bug in the app bootstrap by commenting out the consumer start.
	runCtx, runCancel := context.WithCancel(context.Background())
	consumerDone := make(chan struct{})
	runtime.runCancel = runCancel
	runtime.consumerDone = consumerDone
	go func() {
		defer close(consumerDone)
		kafka.RunConsumer(runCtx, kafkaConsumer, svc)
	}()

	if err := kafka.WaitForConsumerGroupReady(ctx,
		cfg.KafkaBrokers[0],
		kafka.ConsumerGroupID,
		kafka.OutboundPaymentsTopic,
		10*time.Second,
	); err != nil {
		return runtime, err
	}

	log.Info("app started", "base_url", runtime.BaseURL)
	return runtime, nil
}

// ServerErrors returns a channel that reports unexpected HTTP server failures.
func (r *Runtime) ServerErrors() <-chan error {
	return r.serverErr
}

// Shutdown stops the consumer loop, shuts down the HTTP server, and closes all owned resources.
func (r *Runtime) Shutdown(ctx context.Context) error {
	r.shutdownOnce.Do(func() {
		var errs []error

		if r.runCancel != nil {
			r.runCancel()
		}
		if r.consumerDone != nil {
			select {
			case <-r.consumerDone:
			case <-ctx.Done():
				errs = append(errs, fmt.Errorf("wait for kafka consumer stop: %w", ctx.Err()))
			}
		}
		if r.httpServer != nil {
			if err := r.httpServer.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errs = append(errs, fmt.Errorf("shutdown http server: %w", err))
			}
		}
		if r.kafkaConsumer != nil {
			if err := r.kafkaConsumer.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close kafka consumer: %w", err))
			}
		}
		if r.kafkaProducer != nil {
			if err := r.kafkaProducer.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close kafka producer: %w", err))
			}
		}
		if r.redisClient != nil {
			if err := r.redisClient.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close redis client: %w", err))
			}
		}
		if r.pgStore != nil {
			r.pgStore.Close()
		}

		r.shutdownErr = errors.Join(errs...)
	})

	return r.shutdownErr
}

func baseURLFromListener(listener net.Listener) string {
	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		return "http://" + listener.Addr().String()
	}

	if host == "" || host == "::" || host == "0.0.0.0" || strings.HasPrefix(host, "[::]") {
		host = "127.0.0.1"
	}

	return "http://" + net.JoinHostPort(host, port)
}
