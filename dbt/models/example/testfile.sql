{{ config(materialized='table') }}
with
tid as (
  select t.tid_id, t.dato, v.tid_id as virkedag_tid_id, v.dato as virkedag_dato, v.forrige_virkedag_tid_id as virkedag_for_tid_id, 
         v.forrige_virkedag_dato as virkedag_for_dato, v.neste_virkedag_tid_id, v.neste_virkedag_dato
    from {{ source('LGD_SOURCES', 'D_TID') }} t
    join {{ ref('d_virkedag') }} v on t.tid_id < v.neste_virkedag_tid_id and t.tid_id >= v.tid_id
),
konstatert_tap as (
 select rk_bankkunde_id, rk_bankkonto_id, bk_mislighold_id, bk_misligholdstype_id, konstatert_tap_dato, konstatert_tap_belop, scf_gyldig_fom, scf_gyldig_tom from (
  select nvl(kk.rk_bankkunde_id, kt.rk_bankkunde_id) as rk_bankkunde_id,
         nvl(kk.rk_bankkonto_id, kt.rk_bankkonto_id) as rk_bankkonto_id,
         nvl(kk.bk_mislighold_id, kt.bk_mislighold_id) as bk_mislighold_id,
         nvl(kk.bk_misligholdstype_id, kt.bk_misligholdstype_id) as bk_misligholdstype_id,
         nvl(kk.konstatert_tap_dato_korr, kt.konstatert_tap_dato) as konstatert_tap_dato,
         FIRST_VALUE(nvl(kk.konstatert_tap_i_ar_tidl_korr, kt.konstatert_tap_belop)) over (PARTITION BY kt.rk_bankkunde_id ORDER BY kt.bk_misligholdstype_id) konstatert_tap_belop,
         nvl(kk.scf_gyldig_fom, kt.scf_gyldig_fom) as scf_gyldig_fom,
         nvl(kk.scf_gyldig_tom, kt.scf_gyldig_tom) as scf_gyldig_tom    
   from {{ source('LGD_SOURCES', 'F_KONSTATERT_TAP') }} kt
   left join {{ source('LGD_SOURCES', 'F_KONSTATERT_TAP_KORR') }} kk on kt.rk_bankkonto_id = kk.rk_bankkonto_id
                                              and kt.rk_bankkunde_id = kk.rk_bankkunde_id
                                              and kt.bk_mislighold_id = kk.bk_mislighold_id
 )
 group by rk_bankkunde_id, rk_bankkonto_id, bk_mislighold_id, bk_misligholdstype_id, konstatert_tap_dato, konstatert_tap_belop, scf_gyldig_fom, scf_gyldig_tom
),

konto_tap as (
  select ku.sk_bankkunde_biii_id,
         ku.maletidspunkt_kode,
         ku.tid_id,
         ku.batch_navn,
         ku.rk_bankkonto_id,
         kt.konstatert_tap_dato,
         tid.virkedag_dato virkedag_kt_dato,
  --       max(kt.konstatert_tap_belop) keep (dense_rank first order by kt.bk_misligholdstype_id) konstatert_tap_belop
        max(kt.konstatert_tap_belop)
    from {{ ref('m_d_bankkunde_biii_kto_t') }} ku
    join konstatert_tap kt on kt.rk_bankkonto_id = ku.rk_bankkonto_id
                          and kt.konstatert_tap_dato >= ku.sak_start_dato
                          and ku.tid_dato between kt.scf_gyldig_fom and kt.scf_gyldig_tom
    join tid tid on tid.dato = kt.konstatert_tap_dato
    join {{ source('LGD_SOURCES', 'M_KONFIGURASJON_BANK') }} kf on kf.maletidspunkt_kode = ku.maletidspunkt_kode 
                                        and kf.bk_sb1_selskap_id = ku.bk_sb1_selskap_id
                                        and kf.batch_navn = ku.batch_navn
                                        and kf.tid_id = ku.tid_id
    where ku.tid_id = '20230331'
    and ku.batch_navn = 'ETTER_TETTING_AV_F_EAD_T'
   group by ku.sk_bankkunde_biii_id,
            ku.maletidspunkt_kode,
            ku.tid_id,
            ku.batch_navn,
            ku.rk_bankkonto_id,
            kt.konstatert_tap_dato,
            tid.virkedag_dato
)
select * from konto_tap