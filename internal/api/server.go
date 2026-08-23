package api

import (
	"net/http"
	"sync"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	Pool *pgxpool.Pool

	mu         sync.Mutex
	streams    map[uuid.UUID]int
	wakers     map[chan struct{}]struct{}
	stopListen func()
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /auth/register", s.public(s.register))
	mux.HandleFunc("POST /auth/login", s.public(s.login))
	mux.HandleFunc("POST /auth/logout", s.guard(viewer, "", "", s.none, s.logout))

	mux.HandleFunc("POST /orgs", s.guard(viewer, "", "", s.none, s.createOrg))
	mux.HandleFunc("GET /orgs", s.guard(viewer, "", "", s.none, s.listOrgs))
	mux.HandleFunc("GET /orgs/{id}", s.guard(viewer, "", "org", byPath("id", s.orgScope), s.getOrg))
	mux.HandleFunc("PATCH /orgs/{id}", s.guard(admin, "org.update", "org", byPath("id", s.orgScope), s.updateOrg))
	mux.HandleFunc("DELETE /orgs/{id}", s.guard(owner, "org.delete", "org", byPath("id", s.orgScope), s.deleteOrg))
	mux.HandleFunc("POST /orgs/{id}/members", s.guard(admin, "org.add_member", "org", byPath("id", s.orgScope), s.addMember))

	mux.HandleFunc("POST /orgs/{orgId}/projects", s.guard(admin, "project.create", "project", byPath("orgId", s.orgScope), s.createProject))
	mux.HandleFunc("GET /orgs/{orgId}/projects", s.guard(viewer, "", "project", byPath("orgId", s.orgScope), s.listProjects))
	mux.HandleFunc("GET /projects/{id}", s.guard(viewer, "", "project", byPath("id", s.projectScope), s.getProject))
	mux.HandleFunc("PATCH /projects/{id}", s.guard(admin, "project.update", "project", byPath("id", s.projectScope), s.updateProject))
	mux.HandleFunc("DELETE /projects/{id}", s.guard(admin, "project.delete", "project", byPath("id", s.projectScope), s.deleteProject))

	mux.HandleFunc("POST /projects/{projectId}/retry-policies", s.guard(admin, "policy.create", "retry_policy", byPath("projectId", s.projectScope), s.createPolicy))
	mux.HandleFunc("GET /projects/{projectId}/retry-policies", s.guard(viewer, "", "retry_policy", byPath("projectId", s.projectScope), s.listPolicies))
	mux.HandleFunc("GET /retry-policies/{id}", s.guard(viewer, "", "retry_policy", byPath("id", s.policyScope), s.getPolicy))
	mux.HandleFunc("PATCH /retry-policies/{id}", s.guard(admin, "policy.update", "retry_policy", byPath("id", s.policyScope), s.updatePolicy))
	mux.HandleFunc("DELETE /retry-policies/{id}", s.guard(admin, "policy.delete", "retry_policy", byPath("id", s.policyScope), s.deletePolicy))

	mux.HandleFunc("POST /projects/{projectId}/queues", s.guard(admin, "queue.create", "queue", byPath("projectId", s.projectScope), s.createQueue))
	mux.HandleFunc("GET /projects/{projectId}/queues", s.guard(viewer, "", "queue", byPath("projectId", s.projectScope), s.listQueues))
	mux.HandleFunc("GET /queues/{id}", s.guard(viewer, "", "queue", byPath("id", s.queueScope), s.getQueue))
	mux.HandleFunc("PATCH /queues/{id}", s.guard(admin, "queue.update", "queue", byPath("id", s.queueScope), s.updateQueue))
	mux.HandleFunc("DELETE /queues/{id}", s.guard(admin, "queue.delete", "queue", byPath("id", s.queueScope), s.deleteQueue))
	mux.HandleFunc("POST /queues/{id}/pause", s.guard(member, "queue.pause", "queue", byPath("id", s.queueScope), s.pauseQueue))
	mux.HandleFunc("POST /queues/{id}/resume", s.guard(member, "queue.resume", "queue", byPath("id", s.queueScope), s.resumeQueue))

	mux.HandleFunc("POST /queues/{id}/jobs", s.guard(member, "job.submit", "job", byPath("id", s.queueScope), s.submitJob))
	mux.HandleFunc("POST /queues/{id}/jobs/batch", s.guard(member, "job.submit_batch", "batch", byPath("id", s.queueScope), s.submitBatch))
	mux.HandleFunc("GET /jobs", s.guard(viewer, "", "job", s.byProjectOrQueue, s.listJobs))
	mux.HandleFunc("GET /jobs/{id}", s.guard(viewer, "", "job", byPath("id", s.jobScope), s.getJob))
	mux.HandleFunc("POST /jobs/{id}/cancel", s.guard(member, "job.cancel", "job", byPath("id", s.jobScope), s.cancelJob))
	mux.HandleFunc("POST /jobs/{id}/retry", s.guard(member, "job.retry", "job", byPath("id", s.jobScope), s.retryJob))

	mux.HandleFunc("GET /dlq", s.guard(viewer, "", "job", s.byProjectOrQueue, s.listDLQ))
	mux.HandleFunc("POST /dlq/{jobId}/replay", s.guard(member, "job.replay", "job", byPath("jobId", s.jobScope), s.replayJob))

	mux.HandleFunc("GET /projects/{projectId}/events", s.guard(viewer, "", "project", byPath("projectId", s.projectScope), s.listEvents))
	mux.HandleFunc("GET /projects/{projectId}/events/stream", s.streamEvents)
	mux.HandleFunc("GET /workers", s.guard(viewer, "", "worker", s.byProjectQuery, s.listWorkers))
	mux.HandleFunc("GET /stats/queues/{id}", s.guard(viewer, "", "queue", byPath("id", s.queueScope), s.queueStats))
	mux.HandleFunc("GET /batches/{id}", s.guard(viewer, "", "batch", byPath("id", s.batchScope), s.getBatch))

	return s.middleware(mux)
}
