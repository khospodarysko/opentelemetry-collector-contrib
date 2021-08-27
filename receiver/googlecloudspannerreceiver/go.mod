module github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver

go 1.16

require (
	go.opentelemetry.io/collector v0.33.1-0.20210826200354-479f46434f9a
	cloud.google.com/go/spanner v1.24.0
	github.com/stretchr/testify v1.7.0
	go.uber.org/zap v1.18.1
	google.golang.org/api v0.51.0
)
