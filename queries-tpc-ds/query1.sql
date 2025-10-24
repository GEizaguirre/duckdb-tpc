-- start query 1 in stream 0 using template query1.tpl 
-- WITH customer_total_return 
--      AS (SELECT sr_customer_sk     AS ctr_customer_sk, 
--                 sr_store_sk        AS ctr_store_sk, 
--                 Sum(sr_return_amt) AS ctr_total_return 
--          FROM   store_returns, 
--                 date_dim 
--          WHERE  sr_returned_date_sk = d_date_sk 
--                 AND d_year = 2000 
--          GROUP  BY sr_customer_sk, 
--                    sr_store_sk) 
-- SELECT c_customer_id 
-- FROM   customer_total_return ctr1, 
--        store, 
--        customer 
-- WHERE  ctr1.ctr_total_return > (SELECT Avg(ctr_total_return) * 1.2 
--                                 FROM   customer_total_return ctr2 
--                                 WHERE  ctr1.ctr_store_sk = ctr2.ctr_store_sk) 
--        AND s_store_sk = ctr1.ctr_store_sk 
--        AND s_state = 'TN' 
--        AND ctr1.ctr_customer_sk = c_customer_sk 
-- ORDER  BY c_customer_id;

-- WITH customer_store_returns AS (
--   SELECT
--     c.c_customer_id, -- Added customer ID
--     sr1.sr_customer_sk,
--     sr1.sr_store_sk,
--     SUM(sr1.sr_return_amt) AS total_return_amount
--   FROM
--     store_returns sr1
--   JOIN 
--     date_dim ON sr1.sr_returned_date_sk = d_date_sk
--   JOIN 
--     store ON sr1.sr_store_sk = s_store_sk
--   JOIN -- Joined the customer table
--     customer c ON sr1.sr_customer_sk = c.c_customer_sk
--   WHERE
--     d_year = 2000 AND 
--     s_state = 'TN'
--   GROUP BY
--     c.c_customer_id, -- Added to GROUP BY
--     sr1.sr_customer_sk,
--     sr1.sr_store_sk
-- )
-- SELECT *
-- FROM customer_store_returns
-- ORDER BY c_customer_id, total_return_amount DESC;

WITH customer_store_returns AS (
  SELECT
    c.c_customer_id,
    sr1.sr_customer_sk,
    sr1.sr_store_sk,
    SUM(sr1.sr_return_amt) AS total_return_amount
  FROM
    store_returns sr1
  JOIN 
    date_dim ON sr1.sr_returned_date_sk = d_date_sk
  JOIN 
    store ON sr1.sr_store_sk = s_store_sk
  JOIN 
    customer c ON sr1.sr_customer_sk = c.c_customer_sk
  WHERE
    d_year = 2000 AND 
    s_state = 'TN'
  GROUP BY
    c.c_customer_id,
    sr1.sr_customer_sk,
    sr1.sr_store_sk
)
SELECT 
  sr_store_sk, 
  AVG(total_return_amount) AS average_store_return
FROM 
  customer_store_returns
GROUP BY 
  sr_store_sk
ORDER BY 
  sr_store_sk;


