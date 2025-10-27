-- TPC-H Query 6: Forecasting Revenue Change
SELECT
    SUM(l_extendedprice * l_discount) AS revenue
FROM
    lineitem
WHERE
    l_shipdate BETWEEN DATE '1994-01-01'
    AND (Cast('1994-01-01' AS DATE) + INTERVAL 1 YEAR)
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 24;
