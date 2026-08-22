package api

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const apiEventSQL = `insert into events (topic, entity_id, project_id, payload) values ($1, $2, $3, $4::jsonb)`

func emit(ctx context.Context, tx pgx.Tx, topic string, entity, project uuid.UUID, payload map[string]any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, apiEventSQL, topic, entity, project, string(b))
	return err
}
