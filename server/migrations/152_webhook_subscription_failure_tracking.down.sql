ALTER TABLE webhook_subscription
    DROP COLUMN IF EXISTS disabled_reason,
    DROP COLUMN IF EXISTS consecutive_failures;
