type Config struct {
    AppName string

    KafkaBrokers []string

    ElasticsearchURL string

    PostgresURL string

    RedisURL string

    HTTPPort string

    LogLevel string
}