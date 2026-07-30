-- Six accounts across two types, enough realistic pairs to write transfer
-- tests against.
INSERT INTO accounts (name, account_type) VALUES
    ('Alice Checking',    'asset'),
    ('Bob Checking',      'asset'),
    ('Carol Checking',    'asset'),
    ('Merchant Revenue',  'liability'),
    ('Platform Fees',     'liability'),
    ('Reserve Pool',      'asset');
