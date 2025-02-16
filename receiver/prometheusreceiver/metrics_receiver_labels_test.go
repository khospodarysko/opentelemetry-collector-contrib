// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prometheusreceiver

import (
	"testing"

	promcfg "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/model/pdata"
)

const targetExternalLabels = `
# HELP go_threads Number of OS threads created
# TYPE go_threads gauge
go_threads 19`

func TestExternalLabels(t *testing.T) {
	targets := []*testData{
		{
			name: "target1",
			pages: []mockPrometheusResponse{
				{code: 200, data: targetExternalLabels},
			},
			validateFunc: verifyExternalLabels,
		},
	}

	testComponent(t, targets, false, "", func(cfg *promcfg.Config) {
		cfg.GlobalConfig.ExternalLabels = labels.FromStrings("key", "value")
	})
}

func verifyExternalLabels(t *testing.T, td *testData, rms []*pdata.ResourceMetrics) {
	verifyNumValidScrapeResults(t, td, rms)
	require.Greater(t, len(rms), 0, "At least one resource metric should be present")

	wantAttributes := td.attributes
	metrics1 := rms[0].InstrumentationLibraryMetrics().At(0).Metrics()
	ts1 := metrics1.At(0).Gauge().DataPoints().At(0).Timestamp()
	doCompare(t, "scrape-externalLabels", wantAttributes, rms[0], []testExpectation{
		assertMetricPresent("go_threads",
			compareMetricType(pdata.MetricDataTypeGauge),
			[]dataPointExpectation{
				{
					numberPointComparator: []numberPointComparator{
						compareTimestamp(ts1),
						compareDoubleValue(19),
						compareAttributes(map[string]string{"key": "value"}),
					},
				},
			}),
	})
}

const targetLabelLimit1 = `
# HELP test_gauge0 This is my gauge
# TYPE test_gauge0 gauge
test_gauge0{label1="value1",label2="value2"} 10
`

func verifyLabelLimitTarget1(t *testing.T, td *testData, rms []*pdata.ResourceMetrics) {
	//each sample in the scraped metrics is within the configured label_limit, scrape should be successful
	verifyNumValidScrapeResults(t, td, rms)
	require.Greater(t, len(rms), 0, "At least one resource metric should be present")

	want := td.attributes
	metrics1 := rms[0].InstrumentationLibraryMetrics().At(0).Metrics()
	ts1 := metrics1.At(0).Gauge().DataPoints().At(0).Timestamp()

	doCompare(t, "scrape-labelLimit", want, rms[0], []testExpectation{
		assertMetricPresent("test_gauge0",
			compareMetricType(pdata.MetricDataTypeGauge),
			[]dataPointExpectation{
				{
					numberPointComparator: []numberPointComparator{
						compareTimestamp(ts1),
						compareDoubleValue(10),
						compareAttributes(map[string]string{"label1": "value1", "label2": "value2"}),
					},
				},
			},
		),
	})
}

const targetLabelLimit2 = `
# HELP test_gauge0 This is my gauge
# TYPE test_gauge0 gauge
test_gauge0{label1="value1",label2="value2",label3="value3"} 10
`

func verifyFailedScrape(t *testing.T, _ *testData, rms []*pdata.ResourceMetrics) {
	//Scrape should be unsuccessful since limit is exceeded in target2
	for _, rm := range rms {
		metrics := getMetrics(rm)
		assertUp(t, 0, metrics)
	}
}

func TestLabelLimitConfig(t *testing.T) {
	targets := []*testData{
		{
			name: "target1",
			pages: []mockPrometheusResponse{
				{code: 200, data: targetLabelLimit1},
			},
			validateFunc: verifyLabelLimitTarget1,
		},
		{
			name: "target2",
			pages: []mockPrometheusResponse{
				{code: 200, data: targetLabelLimit2},
			},
			validateFunc: verifyFailedScrape,
		},
	}

	testComponent(t, targets, false, "", func(cfg *promcfg.Config) {
		// set label limit in scrape_config
		for _, scrapeCfg := range cfg.ScrapeConfigs {
			scrapeCfg.LabelLimit = 5
		}
	})
}

const targetLabelLimits1 = `
# HELP test_gauge0 This is my gauge
# TYPE test_gauge0 gauge
test_gauge0{label1="value1",label2="value2"} 10

# HELP test_counter0 This is my counter
# TYPE test_counter0 counter
test_counter0{label1="value1",label2="value2"} 1

# HELP test_histogram0 This is my histogram
# TYPE test_histogram0 histogram
test_histogram0_bucket{label1="value1",label2="value2",le="0.1"} 1000
test_histogram0_bucket{label1="value1",label2="value2",le="0.5"} 1500
test_histogram0_bucket{label1="value1",label2="value2",le="1"} 2000
test_histogram0_bucket{label1="value1",label2="value2",le="+Inf"} 2500
test_histogram0_sum{label1="value1",label2="value2"} 5000
test_histogram0_count{label1="value1",label2="value2"} 2500

# HELP test_summary0 This is my summary
# TYPE test_summary0 summary
test_summary0{label1="value1",label2="value2",quantile="0.1"} 1
test_summary0{label1="value1",label2="value2",quantile="0.5"} 5
test_summary0{label1="value1",label2="value2",quantile="0.99"} 8
test_summary0_sum{label1="value1",label2="value2"} 5000
test_summary0_count{label1="value1",label2="value2"} 1000
`

func verifyLabelConfigTarget1(t *testing.T, td *testData, rms []*pdata.ResourceMetrics) {
	verifyNumValidScrapeResults(t, td, rms)
	require.Greater(t, len(rms), 0, "At least one resource metric should be present")

	want := td.attributes
	metrics1 := rms[0].InstrumentationLibraryMetrics().At(0).Metrics()
	ts1 := getTS(metrics1)

	e1 := []testExpectation{
		assertMetricPresent("test_counter0",
			compareMetricType(pdata.MetricDataTypeSum),
			[]dataPointExpectation{
				{
					numberPointComparator: []numberPointComparator{
						compareStartTimestamp(ts1),
						compareTimestamp(ts1),
						compareDoubleValue(1),
						compareAttributes(map[string]string{"label1": "value1", "label2": "value2"}),
					},
				},
			}),
		assertMetricPresent("test_gauge0",
			compareMetricType(pdata.MetricDataTypeGauge),
			[]dataPointExpectation{
				{
					numberPointComparator: []numberPointComparator{
						compareTimestamp(ts1),
						compareDoubleValue(10),
						compareAttributes(map[string]string{"label1": "value1", "label2": "value2"}),
					},
				},
			}),
		assertMetricPresent("test_histogram0",
			compareMetricType(pdata.MetricDataTypeHistogram),
			[]dataPointExpectation{
				{
					histogramPointComparator: []histogramPointComparator{
						compareHistogramStartTimestamp(ts1),
						compareHistogramTimestamp(ts1),
						compareHistogram(2500, 5000, []uint64{1000, 500, 500, 500}),
						compareHistogramAttributes(map[string]string{"label1": "value1", "label2": "value2"}),
					},
				},
			}),
		assertMetricPresent("test_summary0",
			compareMetricType(pdata.MetricDataTypeSummary),
			[]dataPointExpectation{
				{
					summaryPointComparator: []summaryPointComparator{
						compareSummaryStartTimestamp(ts1),
						compareSummaryTimestamp(ts1),
						compareSummary(1000, 5000, [][]float64{{0.1, 1}, {0.5, 5}, {0.99, 8}}),
						compareSummaryAttributes(map[string]string{"label1": "value1", "label2": "value2"}),
					},
				},
			}),
	}
	doCompare(t, "scrape-label-config-test", want, rms[0], e1)
}

const targetLabelNameLimit = `
# HELP test_gauge0 This is my gauge
# TYPE test_gauge0 gauge
test_gauge0{label1="value1",labelNameExceedingLimit="value2"} 10

# HELP test_counter0 This is my counter
# TYPE test_counter0 counter
test_counter0{label1="value1",label2="value2"} 1
`

func TestLabelNameLimitConfig(t *testing.T) {
	targets := []*testData{
		{
			name: "target1",
			pages: []mockPrometheusResponse{
				{code: 200, data: targetLabelLimits1},
			},
			validateFunc: verifyLabelConfigTarget1,
		},
		{
			name: "target2",
			pages: []mockPrometheusResponse{
				{code: 200, data: targetLabelNameLimit},
			},
			validateFunc: verifyFailedScrape,
		},
	}

	testComponent(t, targets, false, "", func(cfg *promcfg.Config) {
		// set label limit in scrape_config
		for _, scrapeCfg := range cfg.ScrapeConfigs {
			scrapeCfg.LabelNameLengthLimit = 20
		}
	})
}

const targetLabelValueLimit = `
# HELP test_gauge0 This is my gauge
# TYPE test_gauge0 gauge
test_gauge0{label1="value1",label2="label-value-exceeding-limit"} 10

# HELP test_counter0 This is my counter
# TYPE test_counter0 counter
test_counter0{label1="value1",label2="value2"} 1
`

func TestLabelValueLimitConfig(t *testing.T) {
	targets := []*testData{
		{
			name: "target1",
			pages: []mockPrometheusResponse{
				{code: 200, data: targetLabelLimits1},
			},
			validateFunc: verifyLabelConfigTarget1,
		},
		{
			name: "target2",
			pages: []mockPrometheusResponse{
				{code: 200, data: targetLabelValueLimit},
			},
			validateFunc: verifyFailedScrape,
		},
	}

	testComponent(t, targets, false, "", func(cfg *promcfg.Config) {
		// set label name limit in scrape_config
		for _, scrapeCfg := range cfg.ScrapeConfigs {
			scrapeCfg.LabelValueLengthLimit = 25
		}
	})
}

//for all metric types, testLabel has empty value
const emptyLabelValuesTarget1 = `
# HELP test_gauge0 This is my gauge
# TYPE test_gauge0 gauge
test_gauge0{id="1",testLabel=""} 19

# HELP test_counter0 This is my counter
# TYPE test_counter0 counter
test_counter0{id="1",testLabel=""} 100

# HELP test_histogram0 This is my histogram
# TYPE test_histogram0 histogram
test_histogram0_bucket{id="1",testLabel="",le="0.1"} 1000
test_histogram0_bucket{id="1",testLabel="",le="0.5"} 1500
test_histogram0_bucket{id="1",testLabel="",le="1"} 2000
test_histogram0_bucket{id="1",testLabel="",le="+Inf"} 2500
test_histogram0_sum{id="1",testLabel=""} 5000
test_histogram0_count{id="1",testLabel=""} 2500

# HELP test_summary0 This is my summary
# TYPE test_summary0 summary
test_summary0{id="1",testLabel="",quantile="0.1"} 1
test_summary0{id="1",testLabel="",quantile="0.5"} 5
test_summary0{id="1",testLabel="",quantile="0.99"} 8
test_summary0_sum{id="1",testLabel=""} 5000
test_summary0_count{id="1",testLabel=""} 1000
`

func verifyEmptyLabelValuesTarget1(t *testing.T, td *testData, rms []*pdata.ResourceMetrics) {
	require.Greater(t, len(rms), 0, "At least one resource metric should be present")

	want := td.attributes
	metrics1 := rms[0].InstrumentationLibraryMetrics().At(0).Metrics()
	ts1 := getTS(metrics1)

	e1 := []testExpectation{
		assertMetricPresent("test_gauge0",
			compareMetricType(pdata.MetricDataTypeGauge),
			[]dataPointExpectation{
				{
					numberPointComparator: []numberPointComparator{
						compareTimestamp(ts1),
						compareDoubleValue(19),
						compareAttributes(map[string]string{"id": "1"}),
					},
				},
			}),
		assertMetricPresent("test_counter0",
			compareMetricType(pdata.MetricDataTypeSum),
			[]dataPointExpectation{
				{
					numberPointComparator: []numberPointComparator{
						compareTimestamp(ts1),
						compareDoubleValue(100),
						compareAttributes(map[string]string{"id": "1"}),
					},
				},
			}),
		assertMetricPresent("test_histogram0",
			compareMetricType(pdata.MetricDataTypeHistogram),
			[]dataPointExpectation{
				{
					histogramPointComparator: []histogramPointComparator{
						compareHistogramStartTimestamp(ts1),
						compareHistogramTimestamp(ts1),
						compareHistogram(2500, 5000, []uint64{1000, 500, 500, 500}),
						compareHistogramAttributes(map[string]string{"id": "1"}),
					},
				},
			}),
		assertMetricPresent("test_summary0",
			compareMetricType(pdata.MetricDataTypeSummary),
			[]dataPointExpectation{
				{
					summaryPointComparator: []summaryPointComparator{
						compareSummaryStartTimestamp(ts1),
						compareSummaryTimestamp(ts1),
						compareSummary(1000, 5000, [][]float64{{0.1, 1}, {0.5, 5}, {0.99, 8}}),
						compareSummaryAttributes(map[string]string{"id": "1"}),
					},
				},
			}),
	}
	doCompare(t, "scrape-empty-label-values-1", want, rms[0], e1)
}

// target has two time series for both gauge and counter, only one time series has a value for the label "testLabel"
const emptyLabelValuesTarget2 = `
# HELP test_gauge0 This is my gauge.
# TYPE test_gauge0 gauge
test_gauge0{id="1",testLabel=""} 19
test_gauge0{id="2",testLabel="foobar"} 2

# HELP test_counter0 This is my counter
# TYPE test_counter0 counter
test_counter0{id="1",testLabel=""} 100
test_counter0{id="2",testLabel="foobar"} 110
`

func verifyEmptyLabelValuesTarget2(t *testing.T, td *testData, rms []*pdata.ResourceMetrics) {
	require.Greater(t, len(rms), 0, "At least one resource metric should be present")

	want := td.attributes
	metrics1 := rms[0].InstrumentationLibraryMetrics().At(0).Metrics()
	ts1 := getTS(metrics1)

	e1 := []testExpectation{
		assertMetricPresent("test_gauge0",
			compareMetricType(pdata.MetricDataTypeGauge),
			[]dataPointExpectation{
				{
					numberPointComparator: []numberPointComparator{
						compareTimestamp(ts1),
						compareDoubleValue(19),
						compareAttributes(map[string]string{"id": "1"}),
					},
				},
				{
					numberPointComparator: []numberPointComparator{
						compareTimestamp(ts1),
						compareDoubleValue(2),
						compareAttributes(map[string]string{"id": "2", "testLabel": "foobar"}),
					},
				},
			}),
		assertMetricPresent("test_counter0",
			compareMetricType(pdata.MetricDataTypeSum),
			[]dataPointExpectation{
				{
					numberPointComparator: []numberPointComparator{
						compareTimestamp(ts1),
						compareDoubleValue(100),
						compareAttributes(map[string]string{"id": "1"}),
					},
				},
				{
					numberPointComparator: []numberPointComparator{
						compareTimestamp(ts1),
						compareDoubleValue(110),
						compareAttributes(map[string]string{"id": "2", "testLabel": "foobar"}),
					},
				},
			}),
	}
	doCompare(t, "scrape-empty-label-values-2", want, rms[0], e1)
}

func TestEmptyLabelValues(t *testing.T) {
	targets := []*testData{
		{
			name: "target1",
			pages: []mockPrometheusResponse{
				{code: 200, data: emptyLabelValuesTarget1},
			},
			validateFunc: verifyEmptyLabelValuesTarget1,
		},
		{
			name: "target2",
			pages: []mockPrometheusResponse{
				{code: 200, data: emptyLabelValuesTarget2},
			},
			validateFunc: verifyEmptyLabelValuesTarget2,
		},
	}
	testComponent(t, targets, false, "")
}

const honorLabelsTarget = `
# HELP test_gauge0 This is my gauge
# TYPE test_gauge0 gauge
test_gauge0{instance="hostname:8080",job="honor_labels_test",testLabel="value1"} 1
`

func verifyHonorLabelsFalse(t *testing.T, td *testData, rms []*pdata.ResourceMetrics) {
	want := td.attributes
	require.Greater(t, len(rms), 0, "At least one resource metric should be present")

	metrics1 := rms[0].InstrumentationLibraryMetrics().At(0).Metrics()
	ts1 := metrics1.At(0).Gauge().DataPoints().At(0).Timestamp()

	doCompare(t, "honor_labels_false", want, rms[0], []testExpectation{
		assertMetricPresent("test_gauge0",
			compareMetricType(pdata.MetricDataTypeGauge),
			[]dataPointExpectation{
				{
					numberPointComparator: []numberPointComparator{
						compareTimestamp(ts1),
						compareDoubleValue(1),
						//job and instance labels must be prefixed with "exported_"
						compareAttributes(map[string]string{"exported_job": "honor_labels_test", "exported_instance": "hostname:8080", "testLabel": "value1"}),
					},
				},
			}),
	})
}

func TestHonorLabelsFalseConfig(t *testing.T) {
	targets := []*testData{
		{
			name: "target1",
			pages: []mockPrometheusResponse{
				{code: 200, data: honorLabelsTarget},
			},
			validateFunc: verifyHonorLabelsFalse,
		},
	}

	testComponent(t, targets, false, "")
}

func verifyHonorLabelsTrue(t *testing.T, td *testData, rms []*pdata.ResourceMetrics) {
	//Test for honor_labels: true is skipped. Currently, the Prometheus receiver is unable to support this config
	//See: https://github.com/open-telemetry/opentelemetry-collector-contrib/issues/5757
	//TODO: Enable this test once issue 5757 is resolved
	t.Skip("skipping test for honor_labels true configuration")

	require.Greater(t, len(rms), 0, "At least one resource metric should be present")

	//job and instance label values should be honored from honorLabelsTarget
	expectedAttributes := td.attributes
	expectedAttributes.Update("job", pdata.NewAttributeValueString("honor_labels_test"))
	expectedAttributes.Update("instance", pdata.NewAttributeValueString("hostname:8080"))
	expectedAttributes.Update("host.name", pdata.NewAttributeValueString("hostname"))
	expectedAttributes.Update("port", pdata.NewAttributeValueString("8080"))

	metrics1 := rms[0].InstrumentationLibraryMetrics().At(0).Metrics()
	ts1 := metrics1.At(0).Gauge().DataPoints().At(0).Timestamp()

	doCompare(t, "honor_labels_true", expectedAttributes, rms[0], []testExpectation{
		assertMetricPresent("test_gauge0",
			compareMetricType(pdata.MetricDataTypeGauge),
			[]dataPointExpectation{
				{
					numberPointComparator: []numberPointComparator{
						compareTimestamp(ts1),
						compareDoubleValue(1),
						compareAttributes(map[string]string{"testLabel": "value1"}),
					},
				},
			}),
	})
}

func TestHonorLabelsTrueConfig(t *testing.T) {
	targets := []*testData{
		{
			name: "honor_labels_test",
			pages: []mockPrometheusResponse{
				{code: 200, data: honorLabelsTarget},
			},
			validateFunc: verifyHonorLabelsTrue,
		},
	}

	testComponent(t, targets, false, "", func(cfg *promcfg.Config) {
		// set label name limit in scrape_config
		for _, scrapeCfg := range cfg.ScrapeConfigs {
			scrapeCfg.HonorLabels = true
		}
	})
}
