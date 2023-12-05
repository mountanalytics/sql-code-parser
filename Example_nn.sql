INSERT INTO db1.Loans (Amount_loan, Underlying_value, Asset_type, Amount, Timestamp)
SELECT
    c.First_underlying_value + c.Second_underlying_value AS Underlying_value,
    c.Asset_type,
    lt.Amount,
    lt.Timestamp
FROM
    db2.Collateral c
JOIN
    db3.Loan_transaction lt ON c.loan_id = lt.loan_id;
ALTER TABLE db1.Loans
ADD COLUMN LtV DECIMAL(10, 2);
UPDATE db1.Loans
SET LtV = Amount_loan / Underlying_value;
