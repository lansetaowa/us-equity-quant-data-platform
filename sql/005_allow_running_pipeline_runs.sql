BEGIN;

-- Week 8.6 Postgres-native daily updates create a running pipeline row
-- before processing ticker/window tasks. Older schema versions allowed only
-- terminal states, so this migration expands the status vocabulary.

DO $$
DECLARE
    constraint_record RECORD;
BEGIN
    FOR constraint_record IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'metadata.pipeline_runs'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) ILIKE '%status%'
    LOOP
        EXECUTE format(
            'ALTER TABLE metadata.pipeline_runs DROP CONSTRAINT %I',
            constraint_record.conname
        );
    END LOOP;
END $$;

ALTER TABLE metadata.pipeline_runs
ADD CONSTRAINT pipeline_runs_status_check
CHECK (
    status IN (
        'pending',
        'started',
        'running',
        'success',
        'failed',
        'skipped'
    )
);

COMMIT;