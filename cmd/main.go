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
	config := partition.Config{
		Tables: []partition.TableConfig{
			{
				Name:              "samples",
				TenantId:          "project_id_asd",
				PartitionType:     partition.TypeRange,
				PartitionBy:       []string{"project_id", "created_at"},
				PartitionInterval: partition.OneDay,
				RetentionPeriod:   partition.OneMonth,
			},
			{
				Name:              "samples",
				TenantId:          "project_id_124",
				PartitionType:     partition.TypeRange,
				PartitionBy:       []string{"project_id", "created_at"},
				PartitionInterval: partition.OneDay,
				RetentionPeriod:   partition.OneMonth * 2,
			},
		},
		SchemaName: "public",
	}

	manager, err := partition.NewManager(nil, config, slog.New(slog.NewTextHandler(os.Stdout, nil)), partition.NewRealClock())
	if err != nil {
		log.Fatal(err)
	}

	// Initialize partition structure
	if err := manager.Initialize(context.Background(), config); err != nil {
		log.Fatal(err)
	}

	// Set up maintenance routine
	go func() {
		ticker := time.NewTicker(24 * time.Hour)
		for range ticker.C {
			if err := manager.Maintain(context.Background()); err != nil {
				log.Printf("maintenance error: %v", err)
			}
		}
	}()
}
