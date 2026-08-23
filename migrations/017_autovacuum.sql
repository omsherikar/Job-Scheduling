alter table jobs set (autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_threshold = 200,
                      autovacuum_analyze_scale_factor = 0.02);
alter table job_executions set (autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_threshold = 200);
alter table job_leases set (autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_threshold = 200);
