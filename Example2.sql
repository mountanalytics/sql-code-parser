-- Step 1: Insert data into db1.Loans from db2.Collateral and db3.Loan_transaction
INSERT INTO db1.Loans (Underlying_value, Asset_type, Amount, Timestamp, LtV, Amount_loan)
SELECT
    c.Underlying_value + c.Second_underlying_value AS Underlying_value,
    c.Asset_type,
    lt.Amount,
    lt.Timestamp,
    Amount_loan / (c.Underlying_value + c.Second_underlying_value) AS LtV
FROM
    db2.Collateral c
JOIN
    db3.Loan_transaction lt ON c.some_common_column = lt.some_common_column;

-- Step 2: Insert data into db4.Customers from db5.Accounts
INSERT INTO db4.Customers (Arrears_balance)
SELECT
    a.Arrears_balance
FROM
    db5.Accounts a;

-- Step 3: Create the new table general_database by joining Loans and Customers
CREATE TABLE db6.general_database AS
SELECT
    L.*,
    C.*
FROM
    db1.Loans L
JOIN
    db4.Customers C ON L.customer_id = C.customer_id;
