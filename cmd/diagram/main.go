package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

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

const keyColumnsSQL = `
select c.relname, a.attname, t.typname,
       bool_or(k.contype = 'p') as pk,
       bool_or(k.contype = 'f') as fk
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  join pg_attribute a on a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped
  join pg_type t on t.oid = a.atttypid
  join pg_constraint k on k.conrelid = c.oid and a.attnum = any(k.conkey)
                      and k.contype in ('p', 'f')
 where n.nspname = 'public' and c.relkind in ('r', 'p')
   and not c.relispartition and c.relname <> 'schema_migrations'
 group by c.relname, a.attname, a.attnum, t.typname
 order by c.relname, a.attnum`

const relationsSQL = `
select src.relname, tgt.relname, k.confdeltype, bool_and(a.attnotnull),
       string_agg(a.attname, ', ' order by a.attnum)
  from pg_constraint k
  join pg_class src on src.oid = k.conrelid
  join pg_class tgt on tgt.oid = k.confrelid
  join pg_namespace n on n.oid = src.relnamespace
  join pg_attribute a on a.attrelid = src.oid and a.attnum = any(k.conkey)
 where k.contype = 'f' and n.nspname = 'public' and not src.relispartition
 group by src.relname, tgt.relname, k.conname, k.confdeltype
 order by src.relname, tgt.relname`

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

var onDelete = map[string]string{
	"a": "no action",
	"r": "restrict",
	"c": "cascade",
	"n": "set null",
	"d": "set default",
}

func main() {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		log.Fatal("DATABASE_URL is not set")
	}
	out := "DIAGRAMS.md"
	if len(os.Args) > 1 {
		out = os.Args[1]
	}

	ctx := context.Background()
	pool, err := db.Open(ctx, url, 1, 0)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	var b strings.Builder
	b.WriteString("# Diagrams\n\n")
	b.WriteString("Generated from a migrated database by `make diagram`. Everything here is read\n")
	b.WriteString("out of the live catalog, so it cannot drift from the schema it describes.\n\n")

	lifecycle(ctx, pool, &b)
	entities(ctx, pool, &b)
	tableGraph(ctx, pool, &b)

	if err := os.WriteFile(out, []byte(b.String()), 0o644); err != nil {
		log.Fatal(err)
	}
	fmt.Println("wrote", out)
}

func lifecycle(ctx context.Context, pool *pgxpool.Pool, b *strings.Builder) {
	b.WriteString("## The job lifecycle\n\n")
	b.WriteString("Every edge is a row in `job_transitions`. A trigger rejects any move not listed\n")
	b.WriteString("here, so this is the whole state machine and not a summary of it.\n\n")
	b.WriteString("```mermaid\nstateDiagram-v2\n")
	rows, err := pool.Query(ctx, transitionsSQL)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var from, to, note string
		if err := rows.Scan(&from, &to, &note); err != nil {
			log.Fatal(err)
		}
		fmt.Fprintf(b, "    %s --> %s: %s\n", from, to, note)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	b.WriteString("```\n\n")
}

func entities(ctx context.Context, pool *pgxpool.Pool, b *strings.Builder) {
	b.WriteString("## Entities\n\n")
	b.WriteString("Key columns only. The label on each line is what happens to the child when the\n")
	b.WriteString("parent is deleted.\n\n")
	b.WriteString("```mermaid\nerDiagram\n")

	rows, err := pool.Query(ctx, relationsSQL)
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var src, tgt, del, cols string
		var required bool
		if err := rows.Scan(&src, &tgt, &del, &required, &cols); err != nil {
			log.Fatal(err)
		}
		arrow := "||--o{"
		if required {
			arrow = "||--|{"
		}
		fmt.Fprintf(b, "    %s %s %s : %q\n", tgt, arrow, src, cols+", "+onDelete[del])
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	rows, err = pool.Query(ctx, keyColumnsSQL)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	table := ""
	for rows.Next() {
		var name, col, typ string
		var pk, fk bool
		if err := rows.Scan(&name, &col, &typ, &pk, &fk); err != nil {
			log.Fatal(err)
		}
		if name != table {
			if table != "" {
				b.WriteString("    }\n")
			}
			fmt.Fprintf(b, "    %s {\n", name)
			table = name
		}
		mark := "FK"
		if pk && fk {
			mark = "PK, FK"
		} else if pk {
			mark = "PK"
		}
		fmt.Fprintf(b, "        %s %s %s\n", typ, col, mark)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	if table != "" {
		b.WriteString("    }\n")
	}
	b.WriteString("```\n\n")
}

func tableGraph(ctx context.Context, pool *pgxpool.Pool, b *strings.Builder) {
	b.WriteString("## Every table\n\n")
	b.WriteString("Cylinders are partitioned by day.\n\n")
	b.WriteString("```mermaid\ngraph LR\n")
	rows, err := pool.Query(ctx, tablesSQL)
	if err != nil {
		log.Fatal(err)
	}
	for rows.Next() {
		var name, kind string
		if err := rows.Scan(&name, &kind); err != nil {
			log.Fatal(err)
		}
		if kind == "partitioned" {
			fmt.Fprintf(b, "    %s[(%s)]\n", name, name)
			continue
		}
		fmt.Fprintf(b, "    %s[%s]\n", name, name)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	rows, err = pool.Query(ctx, linksSQL)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var src, tgt string
		if err := rows.Scan(&src, &tgt); err != nil {
			log.Fatal(err)
		}
		fmt.Fprintf(b, "    %s --> %s\n", src, tgt)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	b.WriteString("```\n")
}
