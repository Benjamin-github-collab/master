SELECT level, 
  connect_by_root sk_bankkunde_biii_id as parent_sk_bankkunde_biii_id, 
  connect_by_root rk_bankkunde_id as parent_rk_bankkunde_id, 
  connect_by_root sak_start_dato as parent_sak_start_dato,
  SYS_CONNECT_BY_PATH(kundesak_nr, ' -> '), 
  k.*
  FROM {{ ref('v_d_bankkunde_biii_9mnd') }} k
    START WITH sk_bankkunde_biii_id in (select sk_bankkunde_biii_id from {{ ref('v_d_bankkunde_biii_9mnd_del2') }} )
    CONNECT BY prior k.rk_bankkunde_id = k.rk_bankkunde_id
           and prior k.kundesak_nr + 1 = k.kundesak_nr
           and months_between(k.sak_start_dato, prior k.sak_avsluttet_dato) <= 9;


