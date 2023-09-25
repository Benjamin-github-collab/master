
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

-- Trenger å identifisere rader der man kan starte, dvs. rader der det ikke finnes en foregående sak man er innenfor 9 mnd av
kunde_start as (
  select k2.sk_bankkunde_biii_id, k2.rk_bankkunde_id
    from kunde_med_avsl k2
    left join kunde_med_avsl k3 on k3.rk_bankkunde_id = k2.rk_bankkunde_id
                      and k3.sk_bankkunde_biii_id <> k2.sk_bankkunde_biii_id
                      and k3.kundesak_nr < k2.kundesak_nr -- Sorterer på kundesak_nr som substitutt for sak_start_dato, men som også håndterer to påfølgende like start-datoer
                      and months_between(k2.sak_start_dato, k3.sak_avsluttet_dato) <= 9
   where k3.sk_bankkunde_biii_id is null
)

select * from kunde_start