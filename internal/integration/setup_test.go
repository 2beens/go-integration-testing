package integration_test

import (
	"context"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/2beens/simple-go-service/internal/app"
	"github.com/2beens/simple-go-service/internal/db"
	"github.com/2beens/simple-go-service/internal/db/repo"
	"github.com/2beens/simple-go-service/internal/kafka"
	redisclient "github.com/2beens/simple-go-service/internal/redis"

	segkafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/suite"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

func TestPaymentsSuite(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION_TESTS") != "1" {
		t.Skip("integration tests disabled (set RUN_INTEGRATION_TESTS=1 to run)")
	}
	suite.Run(t, new(PaymentsSuite))
}

// PaymentsSuite starts PostgreSQL, Redis, and Kafka, then boots the real app against them.
type PaymentsSuite struct {
	suite.Suite

	// App under test: shared runtime started with the same bootstrap used by main.
	appRuntime *app.Runtime
	serverURL  string

	// Data stores: Postgres (payments repo), Redis (idempotency). Tests assert DB/Redis state.
	pg           *db.Postgres
	paymentsRepo *repo.PaymentsRepo
	redisClient  *redisclient.Client

	// Kafka: broker address for publish/consume helpers used by the tests.
	kafkaBroker string

	// Form3 mock: server receiving create requests; recorder for asserting payload in tests.
	form3Server   *httptest.Server
	form3Recorder *form3RequestRecorder

	// Testcontainers; kept for TearDownSuite to terminate.
	pgCtr    *tcpostgres.PostgresContainer
	redisCtr *tcredis.RedisContainer
	kafkaCtr *tckafka.KafkaContainer

	// Suite debug output; used in TearDown when closing resources.
	log *slog.Logger
}

func (s *PaymentsSuite) SetupSuite() {
	ctx := context.Background()
	setupComplete := false
	// On setup failure, defer runs TearDownSuite so we don't leave containers running.
	defer func() {
		if !setupComplete {
			s.TearDownSuite()
		}
	}()

	slogTextHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})
	s.log = slog.New(slogTextHandler)

	pgDSN, redisAddr, kafkaBrokers := s.startContainers(ctx)
	s.ensureKafkaTopics(ctx, kafkaBrokers[0])
	s.startForm3Mock()
	s.startApp(ctx, pgDSN, redisAddr, kafkaBrokers)
	s.connectAssertionClients(ctx, pgDSN, redisAddr, kafkaBrokers[0])

	setupComplete = true
}

// startContainers starts Postgres, Redis, and Kafka containers and returns connection info.
func (s *PaymentsSuite) startContainers(ctx context.Context) (pgDSN, redisAddr string, kafkaBrokers []string) {
	s.log.Info("setup: starting containers")

	pgCtr, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("testdb"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
	)
	s.Require().NoError(err, "start postgres container")
	s.pgCtr = pgCtr
	s.log.Info("setup: postgres started")

	redisCtr, err := tcredis.Run(ctx, "redis:7-alpine")
	s.Require().NoError(err, "start redis container")
	s.redisCtr = redisCtr
	s.log.Info("setup: redis started")

	kafkaCtr, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0")
	s.Require().NoError(err, "start kafka container")
	s.kafkaCtr = kafkaCtr
	s.log.Info("setup: kafka started")

	pgDSN, err = pgCtr.ConnectionString(ctx, "sslmode=disable")
	s.Require().NoError(err, "postgres connection string")

	redisURL, err := redisCtr.ConnectionString(ctx)
	s.Require().NoError(err, "redis connection string")
	redisAddr, _ = strings.CutPrefix(redisURL, "redis://")

	kafkaBrokers, err = kafkaCtr.Brokers(ctx)
	s.Require().NoError(err, "kafka brokers")
	return pgDSN, redisAddr, kafkaBrokers
}

// ensureKafkaTopics creates outbound.payments and payment.status topics.
func (s *PaymentsSuite) ensureKafkaTopics(ctx context.Context, broker string) {
	s.Require().NoError(createKafkaTopic(ctx, broker, kafka.OutboundPaymentsTopic))
	s.Require().NoError(createKafkaTopic(ctx, broker, kafka.PaymentStatusTopic))
	s.log.Info("setup: kafka topics created")
}

// connectAssertionClients connects separate Postgres/Redis clients used only for test assertions.
func (s *PaymentsSuite) connectAssertionClients(ctx context.Context, pgDSN, redisAddr, kafkaBroker string) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	pgStore, err := db.New(ctx, pgDSN)
	s.Require().NoError(err, "connect postgres")
	s.pg = pgStore
	s.log.Info("setup: postgres connected")

	s.paymentsRepo = repo.NewPaymentsRepo(pgStore.Pool())

	redisClient, err := redisclient.New(ctx, redisclient.Config{Addr: redisAddr})
	s.Require().NoError(err, "connect redis")
	s.redisClient = redisClient
	s.log.Info("setup: redis connected")

	s.kafkaBroker = kafkaBroker
}

func (s *PaymentsSuite) startForm3Mock() {
	s.form3Recorder = newForm3RequestRecorder()
	s.form3Server = newForm3MockServer(s.form3Recorder)
	s.log.Info("setup: form3 mock server started")
}

func (s *PaymentsSuite) startApp(ctx context.Context, pgDSN, redisAddr string, kafkaBrokers []string) {
	appRuntime, err := app.Start(ctx, s.log, app.Config{
		HTTPAddr:     "127.0.0.1:0",
		PostgresDSN:  pgDSN,
		RedisAddr:    redisAddr,
		KafkaBrokers: kafkaBrokers,
		Form3BaseURL: s.form3Server.URL,
	})
	s.Require().NoError(err, "start app runtime")
	s.appRuntime = appRuntime
	s.serverURL = appRuntime.BaseURL
	s.log.Info("setup: app server started")
}

func (s *PaymentsSuite) TearDownSuite() {
	ctx := context.Background()
	if s.log != nil {
		s.log.Info("teardown: closing resources and terminating containers")
	}

	if s.appRuntime != nil {
		shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if err := s.appRuntime.Shutdown(shutdownCtx); err != nil && s.log != nil {
			s.log.Warn("shutdown app runtime", "err", err)
		}
		cancel()
	}
	if s.form3Server != nil {
		s.form3Server.Close()
	}
	if s.redisClient != nil {
		if err := s.redisClient.Close(); err != nil && s.log != nil {
			s.log.Warn("close redis client", "err", err)
		}
	}
	if s.pg != nil {
		s.pg.Close()
	}
	if s.kafkaCtr != nil {
		if err := s.kafkaCtr.Terminate(ctx); err != nil && s.log != nil {
			s.log.Warn("terminate kafka container", "err", err)
		}
	}
	if s.redisCtr != nil {
		if err := s.redisCtr.Terminate(ctx); err != nil && s.log != nil {
			s.log.Warn("terminate redis container", "err", err)
		}
	}
	if s.pgCtr != nil {
		if err := s.pgCtr.Terminate(ctx); err != nil && s.log != nil {
			s.log.Warn("terminate postgres container", "err", err)
		}
	}
	if s.log != nil {
		s.log.Info("teardown: complete")
	}
}

func createKafkaTopic(ctx context.Context, broker, topic string) error {
	log := slog.Default().With("component", "integration.createKafkaTopic")

	conn, err := segkafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		return fmt.Errorf("dial kafka: %w", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Warn("close kafka conn after create topic", "err", err)
		}
	}()

	return conn.CreateTopics(segkafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
}
