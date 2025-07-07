CREATE OR REPLACE FUNCTION bid_winner_set(a_id INTEGER) RETURNS VOID LANGUAGE sql AS
$_$
-- a_id: ID тендера
-- функция рассчитывает победителей заданного тендера
-- и заполняет поля bid.is_winner и bid.win_amount


-- Если для каждого tender_id продразумевается то, что функция должна запускаться только один раз — то 1-й UPDATE можно убрать + возможно, отказаться от блокировки FOR UPDATE 

UPDATE /*test.*/bid b
SET 
	is_winner = false,
	win_amount = null
WHERE 1 = 1
	AND b.tender_id = a_id;

WITH RECURSIVE cte_src AS (
	SELECT
		t.id AS tender_id,
		t.product_id,
		t.amount AS total_qty,
		t.start_price,
		t.bid_step,
		b.id AS id_bid,
		b.amount AS bid_qty,
		b.price
	FROM /*test.*/tender_product t
	JOIN /*test.*/bid b ON b.tender_id = t.id AND b.product_id = t.product_id
	WHERE t.id = a_id
		AND b.price >= t.start_price
	FOR UPDATE OF t, b NOWAIT
),
	cte_data AS MATERIALIZED (
	SELECT 
		s.id_bid,
		s.tender_id,
		s.product_id,
		s.total_qty,
		s.bid_qty,
		s.start_price,
		s.bid_step,
		s.price,
		ROW_NUMBER() OVER (PARTITION BY s.product_id ORDER BY s.price DESC, s.id_bid) AS bid_rn
	FROM cte_src s
),
cte_recursive_calc AS (
	SELECT 
		d.id_bid,
		d.tender_id,
		d.product_id,
		d.bid_qty,
		d.price,
		d.total_qty,
		d.start_price AS current_price,
		d.bid_step,
		LEAST(d.bid_qty, d.total_qty) AS win_qty,
		GREATEST(d.total_qty - d.bid_qty, 0) AS balance,
		d.bid_rn,
		CASE 
			WHEN d.bid_qty > d.total_qty THEN d.start_price + d.bid_step 
			ELSE d.start_price 
		END AS new_price
	FROM cte_data d
	WHERE d.bid_rn = 1
---
	UNION ALL
---
	SELECT 
		d.id_bid,
		d.tender_id,
		d.product_id,
		d.bid_qty,
		d.price,
		rc.total_qty,
		rc.new_price AS current_price,
		rc.bid_step,
		CASE 
			WHEN rc.balance <= 0 THEN 0
			WHEN d.price >= rc.new_price THEN 
				LEAST(d.bid_qty, rc.balance)
			ELSE 0
		END AS win_qty,
		CASE 
			WHEN rc.balance <= 0 THEN 0
			WHEN d.price >= rc.new_price THEN 
				GREATEST(rc.balance - LEAST(d.bid_qty, rc.balance), 0)
			ELSE rc.balance
		END AS balance,
		d.bid_rn,
		CASE 
			WHEN rc.balance <= 0 THEN rc.new_price
			WHEN d.price >= rc.new_price AND d.bid_qty > rc.balance THEN 
				rc.new_price + rc.bid_step
			ELSE rc.new_price
		END AS new_price
	FROM cte_recursive_calc rc
	JOIN cte_data d ON d.product_id = rc.product_id 
		AND d.bid_rn = rc.bid_rn + 1 
		AND rc.balance > 0)
---
UPDATE /*test.*/bid b
SET 
	is_winner = (r.win_qty > 0),
	win_amount = 
	CASE 
		WHEN r.win_qty = 0 OR r.win_qty = b.amount THEN NULL 
		ELSE r.win_qty 
	END
FROM cte_recursive_calc r
WHERE b.id = r.id_bid
	AND b.tender_id = r.tender_id
	AND b.product_id = r.product_id;
$_$;
