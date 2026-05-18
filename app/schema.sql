-- Migranix Data Platform — Supabase Schema

-- Saved Connections Table (encrypted credentials)
CREATE TABLE IF NOT EXISTS saved_connections (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    db_type TEXT NOT NULL,
    credentials_encrypted TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_used TIMESTAMPTZ DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE
);

-- Enable Row Level Security
ALTER TABLE saved_connections ENABLE ROW LEVEL SECURITY;

-- RLS Policy: Users can only see their own connections
CREATE POLICY "Users can only access their own connections"
    ON saved_connections
    FOR ALL
    USING (auth.uid() = user_id);

-- Index for faster lookups
CREATE INDEX IF NOT EXISTS idx_saved_connections_user_id 
    ON saved_connections(user_id);

CREATE INDEX IF NOT EXISTS idx_saved_connections_last_used 
    ON saved_connections(last_used DESC);

-- Query History Table (optional, if user chooses to save)
CREATE TABLE IF NOT EXISTS query_history (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    connection_id UUID NOT NULL REFERENCES saved_connections(id) ON DELETE CASCADE,
    query TEXT NOT NULL,
    query_name TEXT,
    execution_time_ms INTEGER,
    rows_returned INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE query_history ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Users can only access their own query history"
    ON query_history
    FOR ALL
    USING (auth.uid() = user_id);

CREATE INDEX IF NOT EXISTS idx_query_history_user_id 
    ON query_history(user_id);

CREATE INDEX IF NOT EXISTS idx_query_history_connection_id 
    ON query_history(connection_id);
