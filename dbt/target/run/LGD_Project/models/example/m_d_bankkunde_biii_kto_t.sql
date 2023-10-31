
  
    

        create or replace transient table RISIKO.LGD.m_d_bankkunde_biii_kto_t
         as
        (/**********************************************************************************************
Beskrivelse: Viewet sammenstiller relevante konti per kundesak, samt utleder slutt-dato for
             beregning av kontantstrøm iht. til valgt måletidspunkt.

             Viewet tar utgangspunkt i en tid_id, måletidspunkt, aktuelle banker og ev. dato-intervall
             som begrenser saksutvalget definert i p_lgd_last-tabellen.

             Beregn til-dato, som sier når siste dato for kontantstrøm skal inkluderes i sak, settes
             basert på type måletidspunkt, og kan begrenses av angitt tid_id, tapskonstateringsdato på
             kunde, logikk opp mot tapskonstateringsdato på kunde dersom denne ikke scores lenger
             eller et gitt antall måneder fra sak-start-dato.

             Viewet utleder også om kontoen har hatt en annen eier ved inn- eller utgangen av misligholdssaken.

Spesielt: Viewet er forhåndsfiltrert vha. sb1_lgd.z_mbj_p_lgd_last

Tabellgrunnlag:   v_d_bankkunde_biii_korr
                  d_bankkonto
                  d_sb1_selskap_fusjon
                  p_lgd_last
                  p_maletidspunkt
                  f_konstatert_tap

Endringslogg:
Initialier   Dato         Beskrivelse
MBJ          15.12.20     Opprettet view
MJ           19.01.23     Flyttet til tabellstyrt tool-entilen
MJ           23.01.23     Lagt til filter på bankkoder som skal beregnes samt join mot sb1_selskap_fusjonert
                          for å mappe bankkoder som er infusjonert i annen bank riktig.
BLG          23.10.23     I forbindelse med migrering på sky måtte syntaxen for enkelte kolonner endres. 
                          I dette tilfelle gjalt det KONTO_FOM/KONTO_TOM. Måtte dense_rank til konto_grl spørringen 
                          for deretter å benyttes i neste with. Grunnen til dette er at max aggregeringsfunksjon ikke kan brukes 
                          med vindusfunksjonen dense_rank i Snowflake.                            
***********************************************************************************************/



with p_last as
 (select p2.tid_id,
         to_date(p2.tid_id, 'yyyymmdd') dato,
         p2.maletidspunkt_kode,
         p2.sak_start_dato_fra,
         p2.sak_start_dato_til
    from RISIKO.LGD.P_LGD_M_KONFIGURASJON p2),
konto_grl as
 (select ko.rk_bankkonto_id,
         ko.kontonummer,
         ko.rk_bankkunde_id,
         ko.bk_sb1_selskap_eier_id,
         ko.konto_fom_dato,
         ko.konto_tom_dato,
         ko.scd_gyldig_fom,
         ko.scd_gyldig_tom,
         case
           when nvl(lag(ko.rk_bankkunde_id)
                    over(partition by ko.rk_bankkonto_id order by
                         ko.scd_gyldig_fom),
                    ko.rk_bankkunde_id) <> ko.rk_bankkunde_id then
            '1'
           else
            '0'
         end annen_eier_for_flagg,
         case
           when nvl(lead(ko.rk_bankkunde_id)
                    over(partition by ko.rk_bankkonto_id order by
                         ko.scd_gyldig_fom),
                    ko.rk_bankkunde_id) <> ko.rk_bankkunde_id then
            '1'
           else
            '0'
         end annen_eier_etter_flagg, 
         DENSE_RANK() OVER (PARTITION BY KO.RK_BANKKONTO_ID ORDER BY KO.SCD_GYLDIG_FOM DESC) DENSE_RANK

    from RISIKO.LGD.FAKE_D_BANKKONTO ko
   inner join RISIKO.LGD.M_KONFIGURASJON_BANK kb
      on kb.bk_sb1_selskap_id = ko.bk_sb1_selskap_eier_id)
select p2.tid_id,
       p2.dato tid_dato,
       p.kode maletidspunkt_kode,
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
       to_char(ku.sak_start_dato, 'yyyymmdd') sak_start_tid_id,
       ku.sak_start_dato_biii,
       ku.sak_start_dato_biii_siste,
       ku.sak_kilde_init,
       ku.sak_kilde_oppdatert,
       ku.kundesak_antall_9mnd,
       ku.tilfrisket_dato,
       to_char(ku.tilfrisket_dato, 'yyyymmdd') tilfrisket_tid_id,
       ku.tilfrisket_flagg,
       ku.markedssegment_kode,
       ku.historisk_realisasjon_flagg,
       ku.saker_i_sak_antall,
       ku.korrigert_sak_start_dato_flagg,
       ko.rk_bankkonto_id,
       ko.kontonummer,
       greatest(max(ko.annen_eier_for_flagg),
                max(ko.annen_eier_etter_flagg)) annen_eier_i_lgd_db_flagg,
    --   max(ko.konto_fom_dato) keep(dense_rank last order by ko.scd_gyldig_fom) konto_fom_dato,
       max(CASE WHEN ko.DENSE_RANK = 1 then ko.konto_fom_dato
        else NULL end) as konto_fom_dato, 
       to_char(max(case when ko.dense_rank = 1 then ko.konto_fom_dato else NULL end), 'yyyymmdd') as KONTO_FOM_TID_ID, 
    --   to_char(max(ko.konto_fom_dato)
    --           keep(dense_rank last order by ko.scd_gyldig_fom),
    --           'yyyymmdd') konto_fom_tid_id,
      max(CASE WHEN ko.DENSE_RANK = 1 then ko.konto_tom_dato
        else NULL end) as konto_tom_dato, 
    --   max(ko.konto_tom_dato) keep(dense_rank last order by ko.scd_gyldig_fom) konto_tom_dato,
    --   to_char(max(ko.konto_tom_dato)
    --           keep(dense_rank last order by ko.scd_gyldig_fom),
    --           'yyyymmdd') konto_tom_tid_id,
      to_char(max(case when ko.dense_rank = 1 then ko.konto_tom_dato else NULL end), 'yyyymmdd') as KONTO_TOM_TID_ID, 
      case p.kode /* Siste ledd i least(..): Logikken for sak_avsluttet_dato settes i v_d_bankkunde_biii_9mnd. Denne fastsettes ved laveste dato av tilfriskning og siste scoring av kunden pluss karens(3 måneder eller 12 måneder ved OVERFORING_ARSAK_OPPDAT_KODE = TAP). Dette for at ikke kundeengasjement som lever videre uten lån, og aldri blir tilfrisket i LGD-DB, ikke skal gjenvinne betalte gebyrer på f.eks. brukskonto "for alltid".*/
         when 'tap' then least(p2.dato,
                               ku.tilfrisket_dato,
                               nvl(min(kt.konstatert_tap_dato), p2.dato),
                               nvl(ku.sak_avsluttet_dato, p2.dato))
         when 'tid_id' then least(p2.dato,
                                  ku.tilfrisket_dato,
                                  nvl(ku.sak_avsluttet_dato, p2.dato))
         else least(p2.dato,
                    ku.tilfrisket_dato,
                    add_months(ku.sak_start_dato, p.mnd_antall),
                    nvl(ku.sak_avsluttet_dato, p2.dato))
       end beregn_til_dato,
       nvl(ku.sak_avsluttet_dato, p2.dato) beregnet_stans_etter_score,
       'ETTER_TETTING_AV_F_EAD_T' as batch_navn,
       ku.sak_avsluttet_dato
  from RISIKO.LGD.v_d_bankkunde_biii_korr ku
  join p_last p2
    on p2.dato >= ku.sak_start_dato
   and ku.sak_start_dato between p2.sak_start_dato_fra and
       p2.sak_start_dato_til
  join RISIKO.LGD.P_MALETIDSPUNKT p
    on p.kode = p2.maletidspunkt_kode
  join konto_grl ko
    on ko.rk_bankkunde_id = ku.rk_bankkunde_id
   and nvl(ko.konto_tom_dato, to_date('99991231', 'yyyymmdd')) >=
       ku.sak_start_dato /* Konto må ha fantes etter misligholdsstart*/
   and nvl(ko.konto_fom_dato, to_date('00010101', 'yyyymmdd')) <=
       ku.sak_avsluttet_dato /* Konto må ha vært opprettet før tilfriskning*/
   and ko.scd_gyldig_fom <= ku.sak_avsluttet_dato /*Konto må ha vært koblet til kunden før tilfriskning*/
   and ko.scd_gyldig_tom >= ku.sak_start_dato /* Konto må ha vært koblet til etter misligholdets start-dato*/
  left join RISIKO.LGD.F_KONSTATERT_TAP kt
    on kt.rk_bankkonto_id = ko.rk_bankkonto_id
   and kt.konstatert_tap_dato >= ku.sak_start_dato
   and p2.dato between kt.scf_gyldig_fom and kt.scf_gyldig_tom
 group by p2.tid_id,
          p2.dato,
          p.kode,
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
          ku.sak_start_dato_biii,
          ku.sak_start_dato_biii_siste,
          ku.sak_kilde_init,
          ku.sak_kilde_oppdatert,
          ku.kundesak_antall_9mnd,
          ku.tilfrisket_dato,
          ku.tilfrisket_flagg,
          ku.sist_scoret_misl_i_sak_dato,
          ku.markedssegment_kode,
          ku.historisk_realisasjon_flagg,
          ku.saker_i_sak_antall,
          ku.korrigert_sak_start_dato_flagg,
          ko.rk_bankkonto_id,
          ko.kontonummer,
          p.mnd_antall,
          ku.sak_avsluttet_dato
        );
      
  