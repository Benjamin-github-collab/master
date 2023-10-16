WITH RECURSIVE HierarchicalCTE AS (
  SELECT
    k.*,
    1 AS n, -- Initial level
    k.sk_bankkunde_biii_id AS top_sk,
    k.sak_start_dato AS top_sak_start_dato,
    k.sak_kilde_init AS top_sak_kilde_init,
    k.overforing_arsak_init_kode AS top_overforing_arsak_init_kode,
    CASE WHEN k2.sk_bankkunde_biii_id IS NULL THEN 1 ELSE 0 END AS er_siste_flagg
  FROM
    {{ ref('v_d_bankkunde_biii_9mnd') }} k
  LEFT JOIN
    {{ ref('v_d_bankkunde_biii_9mnd_del2') }} k2 ON k.sk_bankkunde_biii_id = k2.sk_bankkunde_biii_id
  
  UNION ALL
  
  SELECT
    k.*,
    h.n + 1 AS n,
    h.top_sk,
    h.top_sak_start_dato,
    h.top_sak_kilde_init,
    h.top_overforing_arsak_init_kode,
    CASE WHEN k.sk_bankkunde_biii_id IS NULL THEN 1 ELSE 0 END AS er_siste_flagg
  FROM
    {{ ref('v_d_bankkunde_biii_9mnd') }} k
  JOIN
    HierarchicalCTE h
  ON
    k.rk_bankkunde_id = h.rk_bankkunde_id
    AND k.kundesak_nr + 1 = h.kundesak_nr
    AND months_between(k.sak_start_dato, h.sak_avsluttet_dato) <= 9
)

select * from HierarchicalCTE



