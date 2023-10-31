
  
    

        create or replace transient table RISIKO.LGD.m_d_bankkunde_biii_lgd_t
         as
        (/**********************************************************************************************
Beskrivelse: Viewet standardiserer viktige saksdatoer til virkedager, samt beregner informasjon
             om konstatert tap per sak iht. konfigurasjon per konto.

             Vi baserer seg på allerede materialisert tabell iht. p_lgd_m_konfigurasjon.

Tabellgrunnlag:  m_d_bankkunde_biii_kto
                 d_tid
                 f_konstatert_tap

Endringslogg:
Initialier   Dato         Beskrivelse
MBJ          15.12.20     Opprettet view
MJ           19.01.23     Flyttet over i tabellstyrt tool-entilen
PB           10.02.23     Joinet inn f_konstatert_tap_korr for å hente konstaterte tap på 
                          kontoer hvor det konstaterte tapet har variert mellom 0 og ikke-0 
                          i sb1_dvh.f_mislighold
BLG          25.10.23     I forbindelse med migrering på sky måtte syntaxen for enkelte kolonner endres. 
                          I dette tilfelle gjalt det KONSTATERT_TAP_BELOP. Måtte dense_rank til konstatert_tap spørringen 
                          for deretter å benyttes i neste with. Grunnen til dette er at max aggregeringsfunksjon ikke kan brukes 
                          med vindusfunksjonen dense_rank i Snowflake.
***********************************************************************************************/

with
tid as (
  select t.tid_id, t.dato, v.tid_id as virkedag_tid_id, v.dato as virkedag_dato, v.forrige_virkedag_tid_id as virkedag_for_tid_id, 
         v.forrige_virkedag_dato as virkedag_for_dato, v.neste_virkedag_tid_id, v.neste_virkedag_dato
    from RISIKO.LGD.D_TID t
    join RISIKO.LGD.d_virkedag v on t.tid_id < v.neste_virkedag_tid_id and t.tid_id >= v.tid_id
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
   from RISIKO.LGD.F_KONSTATERT_TAP kt
   left join RISIKO.LGD.F_KONSTATERT_TAP_KORR kk on kt.rk_bankkonto_id = kk.rk_bankkonto_id
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
        max(kt.konstatert_tap_belop) konstatert_tap_belop
    from RISIKO.LGD.m_d_bankkunde_biii_kto_t ku
    join konstatert_tap kt on kt.rk_bankkonto_id = ku.rk_bankkonto_id
                          and kt.konstatert_tap_dato >= ku.sak_start_dato
                          and ku.tid_dato between kt.scf_gyldig_fom and kt.scf_gyldig_tom
    join tid tid on tid.dato = kt.konstatert_tap_dato
    join RISIKO.LGD.M_KONFIGURASJON_BANK kf on kf.maletidspunkt_kode = ku.maletidspunkt_kode 
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
select ku.tid_id,
       ku.maletidspunkt_kode,
       ku.sk_bankkunde_biii_id,
       ku.sk_bankkunde_biii_id_siste,
       ku.rk_bankkunde_id,
       ku.bk_sb1_selskap_id,
       ku.kundenummer,
       ku.kundenavn,
       ku.edb_kunde_id,
       ku.overforing_arsak_init_kode,
       ku.overforing_arsak_oppdat_kode,
       ku.sak_start_dato,
       ku.sak_start_tid_id,
       ku.sak_start_dato_biii,
       ku.sak_start_dato_biii_siste,
       ku.sak_kilde_init,
       ku.sak_kilde_oppdatert,
       ku.kundesak_antall_9mnd,
       nvl(tid_tfr.virkedag_dato, to_date('99991231', 'yyyymmdd')) as tilfrisket_dato,
       nvl(tid_tfr.virkedag_tid_id, '99991231') as tilfrisket_tid_id,
       case ku.tilfrisket_tid_id
         when '99991231' then '0' else '1' end tilfrisket_flagg,
       ku.tilfrisket_flagg tilfrisket_senere_flagg,
       ku.markedssegment_kode,
       ku.historisk_realisasjon_flagg,
       ku.saker_i_sak_antall,
       ku.korrigert_sak_start_dato_flagg,
       ku.rk_bankkonto_id,
       ku.kontonummer,
       tid_til.virkedag_dato beregn_til_dato,
       tid_til.virkedag_tid_id beregn_til_tid_id,
       min(case when kt.virkedag_kt_dato <= tid_til.virkedag_dato then kt.konstatert_tap_dato end) konstatert_tap_dato,
       sum(case when kt.virkedag_kt_dato <= tid_til.virkedag_dato then kt.konstatert_tap_belop end) konstatert_tap_belop,
       ku.konto_fom_dato,
       ku.konto_fom_tid_id,
       ku.konto_tom_dato,
       ku.konto_tom_tid_id,
       ku.annen_eier_i_lgd_db_flagg,
       ku.tid_dato,
       ku.beregnet_stans_etter_score,
       'batch_navn' as batch_navn
  from RISIKO.LGD.m_d_bankkunde_biii_kto_t ku
  left join konto_tap kt on kt.sk_bankkunde_biii_id = ku.sk_bankkunde_biii_id
                        and kt.maletidspunkt_kode = ku.maletidspunkt_kode
                        and kt.rk_bankkonto_id = ku.rk_bankkonto_id
                        and kt.tid_id = ku.tid_id
                        and kt.batch_navn = ku.batch_navn
  join tid tid_til on tid_til.dato = ku.beregn_til_dato
  join RISIKO.LGD.P_LGD_M_KONFIGURASJON konf on konf.maletidspunkt_kode = ku.maletidspunkt_kode
  left join tid tid_tfr on tid_tfr.dato = ku.tilfrisket_dato
  where ku.tid_id = '20230331'
  and ku.batch_navn = 'batch_navn'
 group by ku.tid_id,
          ku.maletidspunkt_kode,
          ku.sk_bankkunde_biii_id,
          ku.sk_bankkunde_biii_id_siste,
          ku.rk_bankkunde_id,
          ku.bk_sb1_selskap_id,
          ku.kundenummer,
          ku.kundenavn,
          ku.edb_kunde_id,
          ku.overforing_arsak_init_kode,
          ku.overforing_arsak_oppdat_kode,
          ku.sak_start_dato,
          ku.sak_start_tid_id,
          ku.sak_start_dato_biii,
          ku.sak_start_dato_biii_siste,
          ku.sak_kilde_init,
          ku.sak_kilde_oppdatert,
          ku.kundesak_antall_9mnd,
          ku.tilfrisket_dato,
          ku.tilfrisket_tid_id,
          ku.tilfrisket_flagg,
          ku.markedssegment_kode,
          ku.historisk_realisasjon_flagg,
          ku.saker_i_sak_antall,
          ku.korrigert_sak_start_dato_flagg,
          ku.rk_bankkonto_id,
          ku.kontonummer,
          ku.beregn_til_dato,
          tid_til.virkedag_dato,
          tid_til.virkedag_tid_id,
          tid_tfr.virkedag_dato,
          tid_tfr.virkedag_tid_id,
          /*kt2.seneste_konstatert_tap_dato,*/
          ku.konto_fom_dato,
          ku.konto_fom_tid_id,
          ku.konto_tom_dato,
          ku.konto_tom_tid_id,
          ku.annen_eier_i_lgd_db_flagg,
          ku.tid_dato,
          ku.beregnet_stans_etter_score
        );
      
  