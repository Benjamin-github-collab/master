
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='view') }}

with
kunde_uten_avsl as (
  select t.sk_bankkunde_biii_id,
         t.rk_bankkunde_id,
         t.bk_sb1_selskap_id,
         t.kundenummer,
         t.kundenavn,
         t.edb_kunde_id,
         t.overforing_arsak_init_kode,
         t.overforing_arsak_oppdat_kode,
         t.sak_start_dato, 
         t.sak_kilde_init,
         t.sak_kilde_oppdatert,
         t.kundesak_antall,
         nvl(t.tilfrisket_dato, to_date('99991231', 'yyyymmdd')) tilfrisket_dato,
         t.tilfrisket_flagg,
         t.sist_scoret_misl_i_sak_dato,
         t.markedssegment_kode,
         t.scd_gyldig_fom,
         t.scd_gyldig_tom,
         t.scd_aktiv_flagg,
         t.historisk_realisasjon_flagg,
         row_number() over (partition by t.rk_bankkunde_id order by t.sak_start_dato, t.tilfrisket_dato nulls last) kundesak_nr

    from RISIKO.LGD.FAKE_D_BANKKUNDE_BIII t
   where t.sak_start_dato is not null
),
--En ny kolonne blir lagt til. Denne kolonne skal funke som en proxy for avsluttet sak dato. Denne skal videre brukes for beregn til logikk. Kolonnen blir lagd siden nåværende bruk av kun tilfrisket dato er ikke tilstrekkelig i situasjoner hvor saken ikke lenger har eksponering, men er med i lgd.
kunde_med_avsl as (
      select k.*,
      least(k.tilfrisket_dato, nvl(last_day(add_months(k.sist_scoret_misl_i_sak_dato, case k.overforing_arsak_oppdat_kode when 'TAP' then 12 else 3 end)), k.tilfrisket_dato)) sak_avsluttet_dato
   from kunde_uten_avsl k
),

kunde_start  AS (
  SELECT
    k2.sk_bankkunde_biii_id,
    k2.rk_bankkunde_id
  FROM
    kunde_med_avsl k2
  LEFT JOIN
    kunde_med_avsl k3
  ON
    k3.rk_bankkunde_id = k2.rk_bankkunde_id
    AND k3.sk_bankkunde_biii_id <> k2.sk_bankkunde_biii_id
    AND k3.kundesak_nr < k2.kundesak_nr 
    AND months_between(k2.sak_start_dato, k3.sak_avsluttet_dato) <= 9
  WHERE
    k3.sk_bankkunde_biii_id IS NULL
),

kunde_connected as (
   
  SELECT level, 
  connect_by_root sk_bankkunde_biii_id as parent_sk_bankkunde_biii_id, 
  connect_by_root rk_bankkunde_id as parent_rk_bankkunde_id, 
  connect_by_root sak_start_dato as parent_sak_start_dato,
  SYS_CONNECT_BY_PATH(kundesak_nr, ' -> '), 
  k.*
  FROM kunde_med_avsl k
    START WITH sk_bankkunde_biii_id in (select sk_bankkunde_biii_id from kunde_start  )
    CONNECT BY prior k.rk_bankkunde_id = k.rk_bankkunde_id
           and prior k.kundesak_nr + 1 = k.kundesak_nr
           and months_between(k.sak_start_dato, prior k.sak_avsluttet_dato) <= 9
)

select * from kunde_connected
