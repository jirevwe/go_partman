package main

import (
	"context"
	partition "github.com/jirevwe/go_partman"
	"log"
	"log/slog"
	"os"
	"time"
)

func main() {
	config := &partition.Config{
		Tables: []partition.Table{
			{
				Name:              "samples",
				TenantId:          "project_id_asd",
				TenantIdColumn:    "project_id",
				PartitionType:     partition.TypeRange,
				PartitionBy:       "created_at",
				PartitionInterval: partition.OneDay,
				RetentionPeriod:   partition.OneMonth,
			},
			{
				Name:              "samples",
				TenantId:          "project_id_124",
				PartitionType:     partition.TypeRange,
				PartitionBy:       "created_at",
				TenantIdColumn:    "project_id",
				PartitionInterval: partition.OneDay,
				RetentionPeriod:   partition.OneMonth * 2,
			},
		},
		SampleRate: time.Second,
		SchemaName: "public",
	}

	manager, err := partition.NewAndStart(nil, config, slog.New(slog.NewTextHandler(os.Stdout, nil)), partition.NewRealClock())
	if err != nil {
		log.Fatal(err)
	}

	if err = manager.Start(context.Background()); err != nil {
		log.Fatal(err)
	}
}
