CREATE TABLE users (
		id SERIAL PRIMARY KEY,
		name TEXT,
		email TEXT,
		role TEXT,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE users_audit (
		id SERIAL PRIMARY KEY,
		user_id INTEGER,
		changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		changed_by TEXT,
		field_changed TEXT,
		old_value TEXT,
	    new_value TEXT
);

CREATE OR REPLACE FUNCTION log_user_update()
RETURNS TRIGGER AS $$
BEGIN
	IF NEW.name != OLD.name THEN
		INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
		VALUES (OLD.id, (SELECT current_user), 'name', OLD.name, NEW.name);
	END IF;
	IF NEW.email != OLD.email THEN
		INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
		VALUES (OLD.id, (SELECT current_user), 'email', OLD.email, NEW.email);
	END IF;
	IF NEW.role != OLD.role THEN
		INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
		VALUES (OLD.id, (SELECT current_user), 'role', OLD.role, NEW.role);
	END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_log_user_update
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION log_user_update();

INSERT INTO users (name, email, role)
VALUES 
('Ivan Ivanov', 'ivan@example.com', 'admin'),
('Anna Petrova', 'anna@example.com', 'user');

UPDATE users
SET email = 'ivan.new@example.com'
WHERE name = 'Ivan Ivanov';

SELECT * FROM public.users_audit
ORDER BY id ASC;

UPDATE users
SET name = 'My new name is Danis'
WHERE role = 'user';

CREATE EXTENSION pg_cron;

SELECT * FROM cron.job;

CREATE OR REPLACE FUNCTION insert_today_data_to_csv() RETURNS void AS $$
DECLARE
	csv_date_str text := '/tmp/users_audit_export_' || to_char(CURRENT_DATE, 'YYYY-MM-DD') || '.csv';
BEGIN
	EXECUTE format(
		'COPY (SELECT * FROM public.users_audit WHERE changed_at > CURRENT_DATE) TO %L WITH (FORMAT CSV, HEADER)', 
	csv_date_str);
END;
$$ LANGUAGE plpgsql;


SELECT cron.schedule( 'save_csv_today', '0 3 * * *', $$ SELECT insert_today_data_to_csv() $$);