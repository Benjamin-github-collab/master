
  create or replace   view RISIKO.LGD.v_d_bankkunde_biii_korr
  
   as (
    /**********************************************************************************************
Beskrivelse: Korrigerer sak_start_dato i ny misligholdsdefinisjon med tidligste sak_start_dato
             fra ev. saker i gammel definisjon som var åpne på sak_start_dato etter ny definisjon.
             Dette fordi at beregnet gjenvunnet må ta høyde for at banken agerte etter forløpet iht.
             gammel definisjon, og ikke den nye som de på det tidspunktet visste ingenting om.

             Resultatet blir det som er ment å være SB1s misligholdssaksdefinisjon.

             LGD-databasekunder som ikke har noen misligholdssak (kun med pga. realisasjonsinformasjon)
             tas også med i viewet, disse vil bl.a. mangle sak_start_dato.

Tabellgrunnlag:   v_d_bankkunde_biii_9mnd
                  d_bankkunde_biii
                  d_bankkunde
                  d_tid

Endringslogg:
Initialier   Dato         Beskrivelse
MBJ          15.12.20     Opprettet view
BLG          27.02.23     Lagt til kolonnen sak_avsluttet_dato

***********************************************************************************************/
with
virkedag as (
  select lead(tid_id) over (order by tid_id) neste_virkedag_tid_id,
         lead(dato) over (order by tid_id) neste_virkedag_dato,
         tid_id virkedag_tid_id,
         dato virkedag_dato,
         lag(tid_id) over (order by tid_id) virkedag_for_tid_id,
         lag(dato) over (order by tid_id) virkedag_for_dato
    from RISIKO.LGD.d_tid
   where virkedag_flagg = '1'
),
tid as (
  select t.tid_id, t.dato, v.virkedag_tid_id, v.virkedag_dato, v.virkedag_for_tid_id, v.virkedag_for_dato, v.neste_virkedag_tid_id, v.neste_virkedag_dato
    from RISIKO.LGD.d_tid t
    join virkedag v on t.tid_id < v.neste_virkedag_tid_id and t.tid_id >= v.virkedag_tid_id
),
bankkunde_biii_korr as (
  select
   t.sk_bankkunde_biii_id,
   t.sk_bankkunde_biii_id_siste,
   t.rk_bankkunde_id,
   t.bk_sb1_selskap_id,
   t.kundenummer,
   t.kundenavn,
   t.edb_kunde_id,
   t.overforing_arsak_init_kode,
   t.overforing_arsak_oppdat_kode,
   case when t2.sk_bankkunde_biii_id is null
          then least(s.sak_start_dato + 90, t.sak_start_dato)
        else t.sak_start_dato
    end as sak_start_dato,
   t.sak_start_dato as sak_start_dato_biii,
   t.sak_start_dato_siste as sak_start_dato_biii_siste,
   t.sak_kilde_init,
   t.sak_kilde_oppdatert,
   t.kundesak_antall_9mnd,
   t.tilfrisket_dato,
   t.tilfrisket_flagg,
   t.sist_scoret_misl_i_sak_dato,
   t.markedssegment_kode,
   t.historisk_realisasjon_flagg,
   t.saker_i_sak_antall,
   t.siste_kundesak_flagg,
   case when t2.sk_bankkunde_biii_id is null and s.sak_start_dato + 90 < t.sak_start_dato then '1' else '0' end as korrigert_sak_start_dato_flagg,
   row_number() over (partition by t.sk_bankkunde_biii_id order by s.sak_start_dato) as rn, -- rangerer for å kunne finne den tidligste start-datoen fra gammel definisjon, ved overlapp
   t.sak_avsluttet_dato
    from RISIKO.LGD.v_d_bankkunde_biii_9mnd  t
    left join RISIKO.LGD.D_BANKKUNDE s on t.rk_bankkunde_id = s.rk_bankkunde_id
                                   and s.overforing_arsak_init_kode = 'MIS'
                                   and t.sak_start_dato between s.sak_start_dato + 90 and nvl(s.tilfrisket_dato, to_date('99991231', 'yyyymmdd'))
                                   and s.sak_start_dato < to_date('20210101', 'yyyymmdd')
    left join RISIKO.LGD.FAKE_D_BANKKUNDE_BIII t2 on t2.rk_bankkunde_id = t.rk_bankkunde_id
                                          and t2.sak_start_dato < t.sak_start_dato
                                          and t2.sak_start_dato between s.sak_start_dato and nvl(s.tilfrisket_dato, to_date('99991231', 'yyyymmdd'))
)
select t.sk_bankkunde_biii_id,
       t.sk_bankkunde_biii_id_siste,
       t.rk_bankkunde_id,
       t.bk_sb1_selskap_id, 
       t.kundenummer,
       t.kundenavn,
       t.edb_kunde_id,
       case t.korrigert_sak_start_dato_flagg when '1' then 'MIS' else t.overforing_arsak_init_kode end overforing_arsak_init_kode,
       t.overforing_arsak_oppdat_kode,
       mis_tid.virkedag_dato as sak_start_dato,
       t.sak_start_dato_biii,
       t.sak_start_dato_biii_siste,
       t.sak_kilde_init,
       t.sak_kilde_oppdatert,
       t.kundesak_antall_9mnd,
       t.tilfrisket_dato,
       t.tilfrisket_flagg,
       t.sist_scoret_misl_i_sak_dato,
       t.markedssegment_kode,
       t.historisk_realisasjon_flagg,
       t.saker_i_sak_antall,
       t.korrigert_sak_start_dato_flagg,
       t.siste_kundesak_flagg,
       t.sak_avsluttet_dato
  from bankkunde_biii_korr t
  join tid mis_tid on nvl(t.sak_start_dato, t.sak_start_dato_biii) = mis_tid.dato
 where t.rn = 1

union all

select t.sk_bankkunde_biii_id,
       t.sk_bankkunde_biii_id as sk_bankkunde_biii_id_siste,
       t.rk_bankkunde_id,
       t.bk_sb1_selskap_id,
       t.kundenummer,
       t.kundenavn,
       t.edb_kunde_id,
       t.overforing_arsak_init_kode,
       t.overforing_arsak_oppdat_kode,
       t.sak_start_dato,
       t.sak_start_dato as sak_start_dato_biii,
       t.sak_start_dato as sak_start_dato_biii_siste,
       t.sak_kilde_init,
       t.sak_kilde_oppdatert,
       nvl(c.antall, 0) as kundesak_antall_9mnd,
       t.tilfrisket_dato,
       t.tilfrisket_flagg,
       t.sist_scoret_misl_i_sak_dato,
       t.markedssegment_kode,
       t.historisk_realisasjon_flagg,
       0 as saker_i_sak_antall,
       '0' as korrigert_sak_start_dato_flagg,
       case when c.rk_bankkunde_id is not null then '0' else '1' end siste_kundesak_flagg,
       null as sak_avsluttet_dato
  from RISIKO.LGD.FAKE_D_BANKKUNDE_BIII t
  left join (select rk_bankkunde_id, count(8) antall from RISIKO.LGD.FAKE_D_BANKKUNDE_BIII where sak_start_dato is not null group by rk_bankkunde_id) c on t.rk_bankkunde_id = c.rk_bankkunde_id
 where t.sak_start_dato is null
  );

