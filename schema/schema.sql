BEGIN TRANSACTION;

--
-- DROP
--

DROP TABLE IF EXISTS idempotency_key;
DROP TABLE IF EXISTS account;

--
-- RAISE
--

--
-- account
--

CREATE TABLE account (
    id          BIGSERIAL PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    email       VARCHAR(100) UNIQUE,
    updated_at  TIMESTAMPTZ
);

--
-- idempotency_key
--

CREATE TABLE idempotency_key (
    id          BIGSERIAL,
    content     VARCHAR(100) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ,
    account_id  BIGINT REFERENCES account (id),

    PRIMARY KEY (id, created_at)
)
PARTITION BY RANGE (created_at);

CREATE INDEX index_idempotency_key_account_id_content
    ON idempotency_key (account_id, content);

CREATE TABLE idempotency_key_y2019m08d15h00 PARTITION OF idempotency_key
    FOR VALUES FROM ('2019-08-15 00:00') TO ('2019-08-15 04:00');
CREATE TABLE idempotency_key_y2019m08d15h04 PARTITION OF idempotency_key
    FOR VALUES FROM ('2019-08-15 04:00') TO ('2019-08-15 08:00');
CREATE TABLE idempotency_key_y2019m08d15h08 PARTITION OF idempotency_key
    FOR VALUES FROM ('2019-08-15 08:00') TO ('2019-08-15 12:00');
CREATE TABLE idempotency_key_y2019m08d15h12 PARTITION OF idempotency_key
    FOR VALUES FROM ('2019-08-15 12:00') TO ('2019-08-15 16:00');
CREATE TABLE idempotency_key_y2019m08d15h16 PARTITION OF idempotency_key
    FOR VALUES FROM ('2019-08-15 16:00') TO ('2019-08-15 20:00');
CREATE TABLE idempotency_key_y2019m08d15h20 PARTITION OF idempotency_key
    FOR VALUES FROM ('2019-08-15 20:00') TO ('2019-08-15 24:00');

CREATE TABLE idempotency_key_y2019m08d16h00 PARTITION OF idempotency_key
    FOR VALUES FROM ('2019-08-16 00:00') TO ('2019-08-16 04:00');

--
-- Partitioned tables can only guarantee a UNIQUE constraint within any single
-- partition. This function runs as a trigger on an insert in the parent table
-- and uses an advisory lock instead to guarantee uniqueness.
--
-- Taken from:
--     http://blog.ioguix.net/postgresql/2015/02/05/Partitionning-and-constraints-part-1.html
--
CREATE OR REPLACE FUNCTION idempotency_key_unique_account_id_content()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
  PERFORM pg_advisory_xact_lock(
    hashtext('idempotency_key_unique_' || NEW.account_id || '_' || NEW.content));

  IF
    count(1) > 1
    FROM idempotency_key
    WHERE account_id = NEW.account_id AND content = NEW.content
  THEN
    RAISE EXCEPTION 'duplicate key value violates unique constraint "%" ON "%" (partition "%")', 
      TG_NAME, 'idempotency_key', TG_TABLE_NAME
    USING DETAIL = format('Key (account_id, content)=(%s, %s) already exists.',
      NEW.account_id, NEW.content);
  END IF;

  RETURN NULL;
END
$function$;

CREATE CONSTRAINT TRIGGER idempotency_key_unique_account_id_content AFTER INSERT OR UPDATE
    ON idempotency_key
    DEFERRABLE INITIALLY IMMEDIATE
    FOR EACH ROW
    EXECUTE FUNCTION idempotency_key_unique_account_id_content();

--
-- SAMPLE DATA
--

INSERT INTO account
    (email)
VALUES
    ('brandur@example.com');

INSERT INTO idempotency_key
    (content, account_id)
VALUES
    ('key123', 1);

INSERT INTO idempotency_key
    (content, account_id, created_at)
VALUES
    ('key456', 1, '2019-08-15 00:01');

INSERT INTO idempotency_key
    (content, account_id, created_at)
VALUES
    ('key123', 1, '2019-08-15 00:01');

COMMIT TRANSACTION;
