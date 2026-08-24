package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/shrutu0929/fenceline/internal/db"
)

const transitionsSQL = `
select from_status::text, to_status::text, note
  from job_transitions order by from_status, to_status`

const tablesSQL = `
select c.relname,
       case when c.relkind = 'p' then 'partitioned' else 'table' end
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
 where n.nspname = 'public' and c.relkind in ('r', 'p')
   and c.relname <> 'schema_migrations'
   and c.relispartition = false
 order by c.relname`

const linksSQL = `
select src.relname, tgt.relname
  from pg_constraint k
  join pg_class src on src.oid = k.conrelid
  join pg_class tgt on tgt.oid = k.confrelid
  join pg_namespace n on n.oid = src.relnamespace
 where k.contype = 'f' and n.nspname = 'public'
   and src.relispartition = false
 group by src.relname, tgt.relname
 order by src.relname, tgt.relname`

func main() {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		log.Fatal("DATABASE_URL is not set")
	}
	ctx := context.Background()
	pool, err := db.Open(ctx, url, 1, 0)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	fmt.Println("```mermaid")
	fmt.Println("stateDiagram-v2")
	rows, err := pool.Query(ctx, transitionsSQL)
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var from, to, note string
		if err := rows.Scan(&from, &to, &note); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("    %s --> %s: %s\n", from, to, note)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("```")

	fmt.Println()
	fmt.Println("```mermaid")
	fmt.Println("graph LR")
	rows, err = pool.Query(ctx, tablesSQL)
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var name, kind string
		if err := rows.Scan(&name, &kind); err != nil {
			log.Fatal(err)
		}
		if kind == "partitioned" {
			fmt.Printf("    %s[(%s)]\n", name, name)
			continue
		}
		fmt.Printf("    %s[%s]\n", name, name)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	rows, err = pool.Query(ctx, linksSQL)
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var src, tgt string
		if err := rows.Scan(&src, &tgt); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("    %s --> %s\n", src, tgt)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("```")
}
