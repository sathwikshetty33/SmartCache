package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
)

func main() {
	host := "timescaledb" // use "localhost" if running from host
	port := 5432
	user := "smartcache"
	password := "smartcache123"
	dbname := "smartcache"

	psqlInfo := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname,
	)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	createTableSQL := `
CREATE TABLE IF NOT EXISTS cache_logs (
    resource_id TEXT NOT NULL,
    action TEXT NOT NULL,
    hit BOOLEAN NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    id BIGSERIAL,                        -- not unique!
    PRIMARY KEY (resource_id, timestamp)  -- composite PK
);
    `
	_, err = db.Exec(createTableSQL)
	if err != nil {
		panic(err)
	}

	// Convert to hypertable
	_, err = db.Exec(`SELECT create_hypertable('cache_logs', 'timestamp', if_not_exists => TRUE);`)
	if err != nil {
		panic(err)
	}

	// Index for faster queries
	_, err = db.Exec(`CREATE INDEX IF NOT EXISTS idx_cache_logs_res_ts 
                      ON cache_logs(resource_id, timestamp DESC);`)
	if err != nil {
		panic(err)
	}

	fmt.Println("✅ cache_logs hypertable created successfully with PK and index!")
}
