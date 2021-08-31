module github.com/open-telemetry/opentelemetry-collector-contrib/receiver/googlecloudspannerreceiver

go 1.17

require (
	cloud.google.com/go/spanner v1.25.0
	github.com/stretchr/testify v1.7.0
	go.opentelemetry.io/collector v0.33.1-0.20210831015230-f3de064f3845
	go.opentelemetry.io/collector/model v0.33.1-0.20210831015230-f3de064f3845
	go.uber.org/zap v1.19.0
	google.golang.org/api v0.54.0
)
