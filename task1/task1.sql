-- Define a common table expression (CTE) to calculate the latest purchase timestamps
WITH latest_purchases AS (
    SELECT
        o.id AS order_id,
        t.id AS transaction_id,
        v.id AS verification_id,
        MAX(p.uploaded_at) AS last_uploaded_at -- The most recent upload timestamp for the purchase
    FROM purchases p
    LEFT JOIN orders o ON p.order_id = o.id
    LEFT JOIN transactions t ON p.transaction_id = t.id
    LEFT JOIN verification v ON p.verification_id = v.id
    GROUP BY o.id, t.id, v.id
)
-- Main query to fetch details about orders, transactions, and verifications
SELECT
    o.id AS order_id,
    t.id AS transaction_id,
    v.id AS verification_id,
    o.created_at,
    GREATEST(o.updated_at, t.updated_at, v.updated_at) AS updated_at, -- The most recent update timestamp among the three entities
    CURRENT_TIMESTAMP AS uploaded_at -- The current timestamp as the upload timestamp	
FROM orders o
LEFT JOIN transactions t ON o.id = t.order_id
LEFT JOIN verification v ON t.id = v.transaction_id
LEFT JOIN latest_purchases lp ON o.id = lp.order_id
WHERE
    lp.last_uploaded_at IS NULL
    OR o.updated_at > lp.last_uploaded_at
    OR t.updated_at > lp.last_uploaded_at
    OR v.updated_at > lp.last_uploaded_at;