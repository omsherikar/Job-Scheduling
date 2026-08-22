package api_test

import (
	"context"
	"testing"

	"github.com/shrutu0929/fenceline/internal/testdb"
)

func TestRBACMatrix(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	tn := setup(t, ctx, pool)
	base := server(t, pool)
	qid := tn.queueID.String()

	tokens := map[string]string{}
	for _, role := range []string{"viewer", "member", "admin", "owner"} {
		_, tok := actor(t, ctx, pool, tn.orgID, role)
		tokens[role] = tok
	}
	nonUID := testdb.NewUser(t, ctx, pool)
	tokens["nonmember"] = testdb.NewSession(t, ctx, pool, nonUID)

	cases := []struct {
		role       string
		read       int
		jobOp      int
		structural int
	}{
		{"viewer", 200, 403, 403},
		{"member", 200, 200, 403},
		{"admin", 200, 200, 200},
		{"owner", 200, 200, 200},
		{"nonmember", 404, 404, 404},
	}

	for _, c := range cases {
		t.Run(c.role, func(t *testing.T) {
			tok := tokens[c.role]

			if st, _, _ := do(t, base, tok, "GET", "/queues/"+qid, nil, nil); st != c.read {
				t.Errorf("read = %d, want %d", st, c.read)
			}

			jobID := testdb.NewJob(t, ctx, pool, tn.projectID, tn.queueID, tn.policyID)
			if st, _, _ := do(t, base, tok, "POST", "/jobs/"+jobID.String()+"/cancel", nil, nil); st != c.jobOp {
				t.Errorf("job op = %d, want %d", st, c.jobOp)
			}

			body := map[string]any{"max_concurrency": 5}
			if st, _, _ := do(t, base, tok, "PATCH", "/queues/"+qid, body, nil); st != c.structural {
				t.Errorf("structural = %d, want %d", st, c.structural)
			}
		})
	}

	testdb.CheckInvariants(t, ctx, pool)
}
