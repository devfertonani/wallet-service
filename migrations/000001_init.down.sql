-- Reverting drops every financial record. Only meant for local environments.
DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS inbox_messages;
DROP TABLE IF EXISTS wallet_ledger_entries;
DROP TABLE IF EXISTS wager_transactions;
DROP TABLE IF EXISTS wallets;
DROP FUNCTION IF EXISTS outbox_events_guard();
DROP FUNCTION IF EXISTS wallets_balance_has_ledger();
DROP FUNCTION IF EXISTS wallets_opening_has_ledger();
DROP FUNCTION IF EXISTS wager_transactions_guard();
DROP FUNCTION IF EXISTS forbid_change();
