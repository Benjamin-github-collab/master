
  create or replace   view RISIKO.LGD.v_d_bankkunde_biii_9mnd_del2
  
   as (
    -- Use the `ref` function to select from other models



WITH V_D_BANKKUNDE_BIII_9MND_DEL2 AS (
  SELECT
    k2.sk_bankkunde_biii_id,
    k2.rk_bankkunde_id
  FROM
    RISIKO.LGD.V_D_BANKKUNDE_BIII_9MND k2
  LEFT JOIN
    RISIKO.LGD.V_D_BANKKUNDE_BIII_9MND k3
  ON
    k3.rk_bankkunde_id = k2.rk_bankkunde_id
    AND k3.sk_bankkunde_biii_id <> k2.sk_bankkunde_biii_id
    AND k3.kundesak_nr < k2.kundesak_nr 
    AND months_between(k2.sak_start_dato, k3.sak_avsluttet_dato) <= 9
  WHERE
    k3.sk_bankkunde_biii_id IS NULL
)

-- Now you can select from the CTE:
SELECT *
FROM V_D_BANKKUNDE_BIII_9MND_DEL2
  );

