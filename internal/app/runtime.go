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

// Config contains the runtime dependencies and addresses needed to start the app.
type Config struct {
	HTTPAddr     string
	PostgresDSN  string
	RedisAddr    string
	KafkaBrokers []string
	Form3BaseURL string
}

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
func Start(ctx context.Context, log *slog.Logger, cfg Config) (*Runtime, error) {
	if log == nil {
		log = slog.Default()
	}
	if len(cfg.KafkaBrokers) == 0 {
		return nil, errors.New("kafka brokers are required")
	}

	pgStore, err := db.New(ctx, cfg.PostgresDSN)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	cleanup := func() {
		pgStore.Close()
	}

	// Note: here we can simulate a bug in the app bootstrap by commenting out the migrations.
	// Unit tests would still pass, but the app would be broken.
	// That's why we have integration tests to catch these kinds of bugs.
	if err := pgStore.RunMigrations(ctx); err != nil {
		cleanup()
		return nil, fmt.Errorf("run migrations: %w", err)
	}

	redisClient, err := redisclient.New(ctx, redisclient.Config{Addr: cfg.RedisAddr})
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("connect redis: %w", err)
	}
	cleanup = func() {
		if err := redisClient.Close(); err != nil {
			log.Warn("close redis client during startup cleanup", "err", err)
		}
		pgStore.Close()
	}

	kafkaProducer := kafka.NewProducer(cfg.KafkaBrokers)
	cleanup = func() {
		if err := kafkaProducer.Close(); err != nil {
			log.Warn("close kafka producer during startup cleanup", "err", err)
		}
		if err := redisClient.Close(); err != nil {
			log.Warn("close redis client during startup cleanup", "err", err)
		}
		pgStore.Close()
	}

	kafkaConsumer := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: cfg.KafkaBrokers,
		GroupID: kafka.ConsumerGroupID,
		Topic:   kafka.OutboundPaymentsTopic,
	})
	cleanup = func() {
		if err := kafkaConsumer.Close(); err != nil {
			log.Warn("close kafka consumer during startup cleanup", "err", err)
		}
		if err := kafkaProducer.Close(); err != nil {
			log.Warn("close kafka producer during startup cleanup", "err", err)
		}
		if err := redisClient.Close(); err != nil {
			log.Warn("close redis client during startup cleanup", "err", err)
		}
		pgStore.Close()
	}

	httpClient := &http.Client{Timeout: 10 * time.Second}
	form3Client := payment.New(cfg.Form3BaseURL, httpClient)
	paymentsRepo := repo.NewPaymentsRepo(pgStore.Pool())
	svc := outbound.NewService(paymentsRepo, redisClient, form3Client, kafkaProducer)
	handler := api.NewHandler(svc)
	router := api.NewRouter(handler)

	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("listen on %q: %w", cfg.HTTPAddr, err)
	}
	cleanup = func() {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Warn("close listener during startup cleanup", "err", err)
		}
		if err := kafkaConsumer.Close(); err != nil {
			log.Warn("close kafka consumer during startup cleanup", "err", err)
		}
		if err := kafkaProducer.Close(); err != nil {
			log.Warn("close kafka producer during startup cleanup", "err", err)
		}
		if err := redisClient.Close(); err != nil {
			log.Warn("close redis client during startup cleanup", "err", err)
		}
		pgStore.Close()
	}

	httpServer := &http.Server{
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	serverErr := make(chan error, 1)
	go func() {
		log.Info("starting HTTP server", "addr", listener.Addr().String())
		if err := httpServer.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
			select {
			case serverErr <- fmt.Errorf("http server: %w", err):
			default:
			}
		}
	}()

	runCtx, runCancel := context.WithCancel(context.Background())
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		kafka.RunConsumer(runCtx, kafkaConsumer, svc)
	}()

	runtime := &Runtime{
		BaseURL:       baseURLFromListener(listener),
		pgStore:       pgStore,
		redisClient:   redisClient,
		kafkaProducer: kafkaProducer,
		kafkaConsumer: kafkaConsumer,
		httpServer:    httpServer,
		serverErr:     serverErr,
		runCancel:     runCancel,
		consumerDone:  consumerDone,
	}

	if err := kafka.WaitForConsumerGroupReady(ctx, cfg.KafkaBrokers[0], kafka.ConsumerGroupID, kafka.OutboundPaymentsTopic, 10*time.Second); err != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if shutdownErr := runtime.Shutdown(shutdownCtx); shutdownErr != nil {
			return nil, errors.Join(err, shutdownErr)
		}
		return nil, err
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
