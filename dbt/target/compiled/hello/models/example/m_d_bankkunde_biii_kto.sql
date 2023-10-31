

with p_last as
 (select p2.tid_id,
         to_date(p2.tid_id, 'yyyymmdd') dato,
         p2.maletidspunkt_kode,
         p2.sak_start_dato_fra,
         p2.sak_start_dato_til
    from sb1_lgd.p_lgd_m_konfigurasjon p2),
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
         end annen_eier_etter_flagg
    from sb1_lgd.d_bankkonto ko
   inner join sb1_lgd.m_konfigurasjon_bank kb
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
       max(ko.konto_fom_dato) keep(dense_rank last order by ko.scd_gyldig_fom) konto_fom_dato,
       to_char(max(ko.konto_fom_dato)
               keep(dense_rank last order by ko.scd_gyldig_fom),
               'yyyymmdd') konto_fom_tid_id,
       max(ko.konto_tom_dato) keep(dense_rank last order by ko.scd_gyldig_fom) konto_tom_dato,
       to_char(max(ko.konto_tom_dato)
               keep(dense_rank last order by ko.scd_gyldig_fom),
               'yyyymmdd') konto_tom_tid_id,
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
       '&batch_navn' as batch_navn,
       ku.sak_avsluttet_dato
  from sb1_lgd.v_d_bankkunde_biii_korr ku
  join p_last p2
    on p2.dato >= ku.sak_start_dato
   and ku.sak_start_dato between p2.sak_start_dato_fra and
       p2.sak_start_dato_til
  join sb1_lgd.p_maletidspunkt p
    on p.kode = p2.maletidspunkt_kode
  join konto_grl ko
    on ko.rk_bankkunde_id = ku.rk_bankkunde_id
   and nvl(ko.konto_tom_dato, to_date('99991231', 'yyyymmdd')) >=
       ku.sak_start_dato /* Konto må ha fantes etter misligholdsstart*/
   and nvl(ko.konto_fom_dato, to_date('00010101', 'yyyymmdd')) <=
       ku.sak_avsluttet_dato /* Konto må ha vært opprettet før tilfriskning*/
   and ko.scd_gyldig_fom <= ku.sak_avsluttet_dato /*Konto må ha vært koblet til kunden før tilfriskning*/
   and ko.scd_gyldig_tom >= ku.sak_start_dato /* Konto må ha vært koblet til etter misligholdets start-dato*/
  left join sb1_lgd.f_konstatert_tap kt
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