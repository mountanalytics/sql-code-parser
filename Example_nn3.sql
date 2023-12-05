INSERT INTO db1.Loans (Underlying_value, Asset_type, Amount, Timestamp, LtV, Amount_loan)
WITH CollateralTransaction AS (
    SELECT
        c.First_underlying_value + c.Second_underlying_value AS Underlying_value,
        c.Asset_type,
        lt.Amount,
        lt.Timestamp,
    FROM
        db2.Collateral c
    JOIN
        db3.Loan_transaction lt ON c.loan_id = lt.loan_id;
)
SELECT
    Underlying_value,
    Asset_type,
    Amount,
    Timestamp,
    Amount_loan / Underlying_value AS LtV
FROM CollateralTransaction;