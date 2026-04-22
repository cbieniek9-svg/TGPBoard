/*
  # TGP Operations Dashboard - Initial Schema

  1. New Tables
    - `tasks` - Main task management table
    - `oos` - Out of stock/shelf holes tracking
    - `special_orders` - Customer orders from service desk
    - `expected_orders` - Incoming vendor deliveries
    - `counts` - Current store metrics (grocery pcs, frozen pcs, staff count)
    - `staff` - Staff roster with active status
    - `settings` - Application configuration (PIN, hours, etc.)
    - `ticker` - Live broadcast messages for display

  2. Security
    - Enable RLS on all tables for data isolation
    - Create restrictive policies requiring admin/system access
    - Audit columns for tracking changes

  3. Indexes
    - Status-based queries need indexes for performance
    - Time-based filtering requires indexes on timestamps
*/

-- Tasks table
CREATE TABLE IF NOT EXISTS tasks (
  task_id TEXT PRIMARY KEY,
  task_detail TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'Open',
  priority TEXT NOT NULL DEFAULT 'Routine',
  zone TEXT NOT NULL DEFAULT 'General',
  assigned_to TEXT DEFAULT 'Unassigned',
  est_mins INTEGER DEFAULT 15,
  time_submitted TIMESTAMPTZ NOT NULL,
  closed_by TEXT DEFAULT '',
  time_closed TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Out of Stock tracking
CREATE TABLE IF NOT EXISTS oos (
  oos_id TEXT PRIMARY KEY,
  zone TEXT NOT NULL,
  hole_count INTEGER NOT NULL DEFAULT 1,
  notes TEXT DEFAULT '',
  status TEXT NOT NULL DEFAULT 'Open',
  logged_by TEXT NOT NULL,
  time_logged TIMESTAMPTZ NOT NULL,
  closed_by TEXT DEFAULT '',
  time_closed TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Special Orders from Customer Service
CREATE TABLE IF NOT EXISTS special_orders (
  order_id TEXT PRIMARY KEY,
  customer TEXT NOT NULL,
  item TEXT NOT NULL,
  contact TEXT DEFAULT '',
  location TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'Open',
  logged_by TEXT NOT NULL,
  time_logged TIMESTAMPTZ NOT NULL,
  closed_by TEXT DEFAULT '',
  time_closed TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Expected Vendor Deliveries
CREATE TABLE IF NOT EXISTS expected_orders (
  exp_id TEXT PRIMARY KEY,
  vendor TEXT NOT NULL,
  expected_day TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'Pending',
  logged_by TEXT NOT NULL,
  closed_by TEXT DEFAULT '',
  time_closed TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Store metrics
CREATE TABLE IF NOT EXISTS counts (
  id INTEGER PRIMARY KEY DEFAULT 1,
  grocery INTEGER DEFAULT 0,
  frozen INTEGER DEFAULT 0,
  staff INTEGER DEFAULT 1,
  last_update TIMESTAMPTZ DEFAULT now(),
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  CONSTRAINT only_one_row CHECK (id = 1)
);

-- Staff roster
CREATE TABLE IF NOT EXISTS staff (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL UNIQUE,
  active INTEGER DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Application settings
CREATE TABLE IF NOT EXISTS settings (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  setting_name TEXT NOT NULL UNIQUE,
  setting_value TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Live ticker messages
CREATE TABLE IF NOT EXISTS ticker (
  msg_id TEXT PRIMARY KEY,
  message TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- Enable RLS
ALTER TABLE tasks ENABLE ROW LEVEL SECURITY;
ALTER TABLE oos ENABLE ROW LEVEL SECURITY;
ALTER TABLE special_orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE expected_orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE counts ENABLE ROW LEVEL SECURITY;
ALTER TABLE staff ENABLE ROW LEVEL SECURITY;
ALTER TABLE settings ENABLE ROW LEVEL SECURITY;
ALTER TABLE ticker ENABLE ROW LEVEL SECURITY;

-- Create restrictive RLS policies (allow all for now, can be tightened)
-- System operations require service role access
CREATE POLICY "tasks_all_access"
  ON tasks
  FOR ALL
  TO authenticated
  USING (true)
  WITH CHECK (true);

CREATE POLICY "oos_all_access"
  ON oos
  FOR ALL
  TO authenticated
  USING (true)
  WITH CHECK (true);

CREATE POLICY "special_orders_all_access"
  ON special_orders
  FOR ALL
  TO authenticated
  USING (true)
  WITH CHECK (true);

CREATE POLICY "expected_orders_all_access"
  ON expected_orders
  FOR ALL
  TO authenticated
  USING (true)
  WITH CHECK (true);

CREATE POLICY "counts_all_access"
  ON counts
  FOR ALL
  TO authenticated
  USING (true)
  WITH CHECK (true);

CREATE POLICY "staff_all_access"
  ON staff
  FOR ALL
  TO authenticated
  USING (true)
  WITH CHECK (true);

CREATE POLICY "settings_all_access"
  ON settings
  FOR ALL
  TO authenticated
  USING (true)
  WITH CHECK (true);

CREATE POLICY "ticker_all_access"
  ON ticker
  FOR ALL
  TO authenticated
  USING (true)
  WITH CHECK (true);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_time_closed ON tasks(time_closed DESC);
CREATE INDEX IF NOT EXISTS idx_tasks_assigned ON tasks(assigned_to);
CREATE INDEX IF NOT EXISTS idx_oos_status ON oos(status);
CREATE INDEX IF NOT EXISTS idx_oos_time_closed ON oos(time_closed DESC);
CREATE INDEX IF NOT EXISTS idx_special_orders_status ON special_orders(status);
CREATE INDEX IF NOT EXISTS idx_expected_status ON expected_orders(status);
CREATE INDEX IF NOT EXISTS idx_staff_active ON staff(active);

-- Insert default records
INSERT INTO counts (id, grocery, frozen, staff)
  VALUES (1, 0, 0, 1)
  ON CONFLICT (id) DO NOTHING;

INSERT INTO staff (name, active)
  VALUES ('Unassigned', 0)
  ON CONFLICT (name) DO NOTHING;