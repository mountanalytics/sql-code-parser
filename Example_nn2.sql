INSERT INTO db1.Loans (Underlying_value, Asset_type, Amount, Timestamp, LtV, Amount_loan)
SELECT
    c.First_underlying_value + c.Second_underlying_value AS Underlying_value,
    c.Asset_type,
    lt.Amount,
    lt.Timestamp,
    Amount_loan / (c.First_underlying_value + c.Second_underlying_value) AS LtV
FROM
    db2.Collateral c
JOIN
    db3.Loan_transaction lt ON c.loan_id = lt.loan_id;