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

package mongoexporter

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/collector/consumer/consumererror"
	jaegertranslator "go.opentelemetry.io/collector/translator/trace/jaeger"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/model/otlp"
	"go.opentelemetry.io/collector/model/pdata"
)

// Marshaler configuration used for marhsaling Protobuf to JSON.
var tracesMarshaler = otlp.NewJSONTracesMarshaler()
var metricsMarshaler = otlp.NewJSONMetricsMarshaler()
var logsMarshaler = otlp.NewJSONLogsMarshaler()

// mongoExporter is the implementation of file exporter that writes telemetry data to a file
// in Protobuf-JSON format.
type mongoExporter struct {
	mongoUlr    string
	tag         string
	mongoClient *mongo.Client
	mongoCol    *mongo.Collection
	mutex       sync.Mutex
}

func (e *mongoExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *mongoExporter) ConsumeTraces(_ context.Context, td pdata.Traces) error {
	batches, err := jaegertranslator.InternalTracesToJaegerProto(td)
	if err != nil {
		return consumererror.Permanent(fmt.Errorf("failed to push trace data via Jaeger exporter: %w", err))
	}

	// Ensure only one write operation happens at a time.
	e.mutex.Lock()
	defer e.mutex.Unlock()

	//var many []interface{}
	for _, m := range batches {
		var foundMethod bool
		var call Call
		for _, span := range m.Spans {
			if span.OperationName == "api.handle" {
				for _, tag := range span.Tags {
					if tag.Key == "method" {
						foundMethod = true
						call.Method = tag.VStr
					}
					if tag.Key == "user" {
						call.Name = tag.VStr
					}
				}
			}
		}
		if !foundMethod {
			continue
		}

		if m.Process != nil {
			call.Service = m.Process.ServiceName
		}

		_, err = e.mongoCol.InsertOne(context.TODO(), call)
		if err != nil {
			return err
		}
	}
	return nil
}

type Call struct {
	Name    string
	Service string
	Method  string
}

func (e *mongoExporter) ConsumeMetrics(_ context.Context, md pdata.Metrics) error {
	return nil
}

func (e *mongoExporter) ConsumeLogs(_ context.Context, ld pdata.Logs) error {
	return nil
}

func (e *mongoExporter) Start(context.Context, component.Host) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var err error
	e.mongoClient, err = mongo.Connect(ctx, options.Client().ApplyURI(e.mongoUlr))
	if err != nil {
		return err
	}
	e.mongoCol = e.mongoClient.Database("traces").Collection("call")
	return nil
}

// Shutdown stops the exporter and is invoked during shutdown.
func (e *mongoExporter) Shutdown(ctx context.Context) error {
	return e.mongoClient.Disconnect(ctx)
}
