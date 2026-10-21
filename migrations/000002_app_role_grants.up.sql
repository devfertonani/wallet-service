-- The service connects as a member of wallet_app, never as the schema owner.
-- The role has no UPDATE, DELETE or TRUNCATE on the ledger, so append-only is
-- enforced twice: by privileges here and by the triggers in 000001.
-- deploy/postgres/init.sql creates the roles; managed databases create them
-- in their own provisioning.
GRANT USAGE ON SCHEMA public TO wallet_app;

GRANT SELECT, INSERT ON wallets, wager_transactions, wallet_ledger_entries, inbox_messages, outbox_events TO wallet_app;
GRANT UPDATE ON wallets, wager_transactions, inbox_messages, outbox_events TO wallet_app;
