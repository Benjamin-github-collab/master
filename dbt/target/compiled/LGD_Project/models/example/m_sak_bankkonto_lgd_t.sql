
with virkedag as (
       select distinct 
            tid.tid_id,
            tid.siste_manedslast_tid_id,
            ku.rk_bankkonto_id,
            p_sikk_mis.batch_navn
       from RISIKO.LGD.m_d_bankkunde_biii_lgd_t ku /* materialisert tabell iht. sb1_lgd.p_lgd_last*/
       join RISIKO.LGD.d_virkedag tid on tid.tid_id = ku.sak_start_tid_id 
       join RISIKO.LGD.P_LGD_BATCH p_sikk_mis on p_sikk_mis.fordelt_sikkerhetsverdi_flagg = '1'
                                          and substr(tid.siste_manedslast_tid_id, 1, 6) between nvl(p_sikk_mis.vintage_irba_fom, '000000') and nvl(p_sikk_mis.vintage_irba_tom, '999999')
                                          and nvl(p_sikk_mis.vintage_irba_fom, p_sikk_mis.vintage_irba_tom) is not null
       where ku.tid_id = '&uttrekksdato'
         and ku.batch_navn = '&batch_navn'
),
sikkerhet as (
       select /*+ materialize parallel(sikk,4)*/ sikk.tid_id, sikk.rk_bankkonto_id, sikk.batch_navn, sum(sikk.fordelt_verdi_sikkerhet) fordelt_verdi_sikk_belop
       from RISIKO.LGD.F_FORDELT_SIKKERHETSVERDI_T sikk
       join virkedag v on v.siste_manedslast_tid_id = sikk.tid_id and v.rk_bankkonto_id = sikk.rk_bankkonto_id and v.batch_navn = sikk.batch_navn
       group by sikk.tid_id, sikk.rk_bankkonto_id, sikk.batch_navn
), 
cashpool as (
       select cp.bk_bankkonto_id, balansekonto_sb1_flagg, cp.scd_gyldig_fom, cp.scd_gyldig_tom
       from RISIKO.LGD.D_CASHPOOLHIERARKI cp 
       where cp.scd_slettet_i_kilde_dato is null
),
kks as (
       select kks.bk_bankkonto_id, kks.scd_gyldig_fom, kks.scd_gyldig_tom
       from RISIKO.LGD.v_reskontro_kks_underkonto kks
),
konto_lgd as (
  select /*+ parallel(ead_mis,4) parallel(ead_mis_irba,4) parallel(bal_mis,4) parallel(bal_tid,4)*/
         ku.tid_id, ku.maletidspunkt_kode, k.kontantstrom_kilde_kode, ku.sk_bankkunde_biii_id, ku.rk_bankkunde_id, ku.rk_bankkonto_id, ku.bk_sb1_selskap_id, ku.kundenummer, ku.kundenavn, ku.overforing_arsak_init_kode,
         ku.overforing_arsak_oppdat_kode, ku.sak_start_dato, ku.tilfrisket_dato, ku.kontonummer, ead_mis.ead_total mislighold_ead_total_daglast,
         case when (k.eksponering_flagg = '1'
                    or cp_mis.balansekonto_sb1_flagg = '1')
               and bal_mis.kontonummer is not null
               and not nvl(cp_mis.balansekonto_sb1_flagg, '1') = '0'
               and kks_mis.bk_bankkonto_id is null
           then -1 * round(least(bal_mis.saldo_nok + nvl(bal_mis.ikkekap_debetrente_belop, 0) + nvl(bal_mis.ikkekap_kreditrente_belop, 0), 0), 0)
           else 0
         end mislighold_saldo_renter_belop,
         case k.gyldig_kontantstrom_flagg
           when '1' then k.naverdi_kontantstrom_belop
           else 0
          end naverdi_kontantstrom_belop,
         case k.gyldig_kontantstrom_flagg
           when '1' then k.kontantstrom_belop
           else 0
          end kontantstrom_belop,
         case when (k.eksponering_flagg = '1'
                    or cp_tid.balansekonto_sb1_flagg = '1')
               and bal_tid.kontonummer is not null
               and not nvl(cp_tid.balansekonto_sb1_flagg, '1') = '0'
               and kks_tid.bk_bankkonto_id is null
               and ku.konstatert_tap_dato is null
                then greatest(-1 * nvl(bal_tid.saldo_nok + nvl(bal_tid.ikkekap_debetrente_belop, 0) + nvl(bal_tid.ikkekap_kreditrente_belop, 0), 0), 0)
              else 0
          end tid_saldo_renter_belop,
         ku.konstatert_tap_dato,
         ku.konstatert_tap_belop,
         case when k.gyldig_kontantstrom_flagg = '1' and k.kontantstrom_dato >= ku.konstatert_tap_dato
           then k.naverdi_kontantstrom_belop
           else 0
          end nv_kontantstr_etter_kt_belop,
         case when k.gyldig_kontantstrom_flagg = '1' and k.kontantstrom_dato >= ku.konstatert_tap_dato
           then k.kontantstrom_belop
           else 0
          end kontantstr_etter_kt_belop,
         ead_mis_irba.ead_total mislighold_vintage_irba_ead, sikk_mis_irba.fordelt_verdi_sikk_belop mislighold_vintage_irba_sikk, k.eksponering_flagg, k.kredittforetak_flagg, k.syndikat_flagg, k.eierbytte_flagg, k.rk_trekkonto_utenfor_lgd_flagg, k.korr_kilde_trans_flagg, k.korr_kilde_gl_flagg,
         k.korr_kilde_konflikt_flagg, k.ekskludert_konto_flagg, k.ekskludert_konto_arsak, k.annet_spesielt, bal_mis.saldo_nok mislighold_saldo_nok, bal_mis.valutakode, bal_mis.ikkekap_kreditrente_belop misl_ikkekap_kreditrente_belop, bal_mis.ikkekap_debetrente_belop misl_ikkekap_debetrente_belop,
         case when cp_mis.balansekonto_sb1_flagg = '0' then '1' else '0' end cp_mis_ikke_balansekonto_flagg,
         case when kks_mis.bk_bankkonto_id is not null then '1' else '0' end kks_mis_underkonto_flagg,
         case when cp_tid.balansekonto_sb1_flagg = '0' then '1' else '0' end cp_tid_ikke_balansekonto_flagg,
         case when kks_tid.bk_bankkonto_id is not null then '1' else '0' end kks_tid_underkonto_flagg,
         k.rente_navn, k.rente_ppoeng, k.rente_referanse_ppoeng, ku.sak_start_tid_id, ku.sak_start_dato_biii, ku.sak_start_dato_biii_siste, ku.sak_kilde_init, ku.sak_kilde_oppdatert, ku.tilfrisket_tid_id, ku.tilfrisket_flagg, ku.tilfrisket_senere_flagg,
         ku.kundesak_antall_9mnd, ku.historisk_realisasjon_flagg, ku.saker_i_sak_antall, ku.markedssegment_kode, ku.korrigert_sak_start_dato_flagg, ku.konto_fom_dato, ku.konto_tom_dato, ku.beregn_til_dato, ku.beregn_til_tid_id, ku.beregnet_stans_etter_score
    from RISIKO.LGD.m_d_bankkunde_biii_lgd_t ku 
    join RISIKO.LGD.M_KONFIGURASJON_BANK kb on kb.maletidspunkt_kode = ku.maletidspunkt_kode
                                        and kb.bk_sb1_selskap_id = ku.bk_sb1_selskap_id
    join RISIKO.LGD.d_virkedag tid_mis on tid_mis.tid_id = ku.sak_start_tid_id
    left join cashpool cp_mis on cp_mis.bk_bankkonto_id = ku.kontonummer
                                               and tid_mis.forrige_virkedag_dato between cp_mis.scd_gyldig_fom and cp_mis.scd_gyldig_tom

    left join kks kks_mis on kks_mis.bk_bankkonto_id = ku.kontonummer
                         and tid_mis.forrige_virkedag_dato between kks_mis.scd_gyldig_fom and kks_mis.scd_gyldig_tom
    left join RISIKO.LGD.F_EAD_T ead_mis on ead_mis.tid_id = tid_mis.forrige_virkedag_tid_id
                                     and ead_mis.rk_bankkonto_id = ku.rk_bankkonto_id
                                     and ead_mis.batch_navn = 'DAGLAST'
    left join RISIKO.LGD.P_LGD_BATCH p_ead_mis on p_ead_mis.ead_flagg = '1'
                                           and substr(tid_mis.siste_manedslast_tid_id, 1, 6) between nvl(p_ead_mis.vintage_irba_fom, '000000') and nvl(p_ead_mis.vintage_irba_tom, '999999')
                                           and nvl(p_ead_mis.vintage_irba_fom, p_ead_mis.vintage_irba_tom) is not null
    left join RISIKO.LGD.F_EAD_T ead_mis_irba on ead_mis_irba.tid_id = tid_mis.siste_manedslast_tid_id
                                          and ead_mis_irba.rk_bankkonto_id = ku.rk_bankkonto_id
                                          and ead_mis_irba.batch_navn = p_ead_mis.batch_navn
    left join sikkerhet sikk_mis_irba on sikk_mis_irba.tid_id = tid_mis.siste_manedslast_tid_id
                                     and sikk_mis_irba.rk_bankkonto_id = ku.rk_bankkonto_id 
    
    left join RISIKO.LGD.F_KONTOBALANSE_T bal_mis on bal_mis.tid_id = tid_mis.forrige_virkedag_tid_id
                                              and bal_mis.rk_bankkonto_id = ku.rk_bankkonto_id
    left join cashpool cp_tid on cp_tid.bk_bankkonto_id = ku.kontonummer
                             and ku.beregn_til_dato between cp_tid.scd_gyldig_fom and cp_tid.scd_gyldig_tom

    left join kks kks_tid on kks_tid.bk_bankkonto_id = ku.kontonummer
                         and ku.beregn_til_dato between kks_tid.scd_gyldig_fom and kks_tid.scd_gyldig_tom
    left join RISIKO.LGD.F_KONTOBALANSE_T bal_tid on bal_tid.tid_id = ku.beregn_til_tid_id
                                              and bal_tid.rk_bankkonto_id = ku.rk_bankkonto_id
    left join RISIKO.LGD.m_sak_bankkonto_kontantstrom_t k on k.tid_id = '&uttrekksdato'
                                                    and k.batch_navn = '&batch_navn'
                                                    and ku.maletidspunkt_kode = k.maletidspunkt_kode
                                                    and ku.bk_sb1_selskap_id = k.bk_sb1_selskap_id
                                                    and ku.sk_bankkunde_biii_id = k.sk_bankkunde_biii_id
                                                    and ku.rk_bankkonto_id = k.rk_bankkonto_id
   where ku.tid_id = '&uttrekksdato'
     and ku.batch_navn = '&batch_navn'
)
select ku.tid_id, ku.maletidspunkt_kode, ku.kontantstrom_kilde_kode, ku.sk_bankkunde_biii_id, ku.rk_bankkunde_id, ku.rk_bankkonto_id, ku.bk_sb1_selskap_id, ku.kundenummer,
       case ku.markedssegment_kode when 'PM' then 'NN (PM)' else ku.kundenavn end kundenavn,
       ku.overforing_arsak_init_kode, ku.overforing_arsak_oppdat_kode, ku.sak_start_dato, ku.tilfrisket_dato, ku.kontonummer, ku.mislighold_ead_total_daglast, ku.mislighold_saldo_renter_belop,
       sum(ku.naverdi_kontantstrom_belop) naverdi_gjenvunnet_belop,
 /*       round(ku.tid_saldo_renter_belop / power(1 + ku.rente_referanse_ppoeng / 100, (ku.beregn_til_dato - ku.sak_start_dato) / 365),2) as naverdi_tid_saldo_renter_belop,*/
        round(disc_naverdi(ku.tid_saldo_renter_belop, ku.rente_referanse_ppoeng, ku.sak_start_dato, ku.beregn_til_dato), 2) naverdi_tid_saldo_renter_belop,
       ku.konstatert_tap_dato, ku.konstatert_tap_belop,
       sum(ku.nv_kontantstr_etter_kt_belop) naverdi_gjenvunnet_kt_belop,
       ku.mislighold_vintage_irba_ead, ku.mislighold_vintage_irba_sikk,
       sum(ku.kontantstrom_belop) gjenvunnet_nominelt_belop,
       sum(ku.kontantstr_etter_kt_belop) gjenvunnet_nominelt_kt_belop,
       ku.eksponering_flagg, ku.kredittforetak_flagg, ku.syndikat_flagg, ku.eierbytte_flagg, ku.rk_trekkonto_utenfor_lgd_flagg, ku.korr_kilde_trans_flagg, ku.korr_kilde_gl_flagg, ku.korr_kilde_konflikt_flagg, ku.ekskludert_konto_flagg,
       ku.ekskludert_konto_arsak, ku.annet_spesielt, ku.mislighold_saldo_nok, ku.valutakode, ku.misl_ikkekap_kreditrente_belop, ku.misl_ikkekap_debetrente_belop, ku.tid_saldo_renter_belop, ku.cp_mis_ikke_balansekonto_flagg, ku.kks_mis_underkonto_flagg,
       ku.cp_tid_ikke_balansekonto_flagg, ku.kks_tid_underkonto_flagg, ku.rente_navn, ku.rente_ppoeng, ku.rente_referanse_ppoeng, ku.sak_start_tid_id, ku.sak_start_dato_biii, ku.sak_start_dato_biii_siste, ku.sak_kilde_init, ku.sak_kilde_oppdatert,
       ku.tilfrisket_tid_id, ku.tilfrisket_flagg, ku.tilfrisket_senere_flagg, ku.kundesak_antall_9mnd, ku.historisk_realisasjon_flagg, ku.saker_i_sak_antall, ku.markedssegment_kode, ku.korrigert_sak_start_dato_flagg, ku.konto_fom_dato,
       ku.konto_tom_dato, ku.beregn_til_dato, ku.beregn_til_tid_id, ku.beregnet_stans_etter_score,
       '&batch_navn' as batch_navn
  from konto_lgd ku
 group by ku.tid_id, ku.maletidspunkt_kode, ku.kontantstrom_kilde_kode, ku.sk_bankkunde_biii_id, ku.rk_bankkunde_id, ku.rk_bankkonto_id, ku.bk_sb1_selskap_id, ku.kundenummer, ku.kundenavn, ku.overforing_arsak_init_kode, ku.overforing_arsak_oppdat_kode,
         ku.sak_start_dato, ku.tilfrisket_dato, ku.kontonummer, ku.mislighold_ead_total_daglast, ku.mislighold_saldo_renter_belop, ku.tid_saldo_renter_belop, ku.konstatert_tap_dato, ku.konstatert_tap_belop, ku.mislighold_vintage_irba_ead,
         ku.mislighold_vintage_irba_sikk, ku.eksponering_flagg, ku.kredittforetak_flagg, ku.syndikat_flagg, ku.eierbytte_flagg, ku.rk_trekkonto_utenfor_lgd_flagg, ku.korr_kilde_trans_flagg, ku.korr_kilde_gl_flagg, ku.korr_kilde_konflikt_flagg,
         ku.ekskludert_konto_flagg, ku.ekskludert_konto_arsak, ku.annet_spesielt, ku.mislighold_saldo_nok, ku.valutakode, ku.misl_ikkekap_kreditrente_belop, ku.misl_ikkekap_debetrente_belop, ku.cp_mis_ikke_balansekonto_flagg, ku.kks_mis_underkonto_flagg,
         ku.cp_tid_ikke_balansekonto_flagg, ku.kks_tid_underkonto_flagg, ku.rente_navn, ku.rente_ppoeng, ku.rente_referanse_ppoeng, ku.sak_start_tid_id, ku.sak_start_dato_biii, ku.sak_start_dato_biii_siste, ku.sak_kilde_init, ku.sak_kilde_oppdatert,
         ku.tilfrisket_tid_id, ku.tilfrisket_flagg, ku.tilfrisket_senere_flagg, ku.kundesak_antall_9mnd, ku.historisk_realisasjon_flagg, ku.saker_i_sak_antall, ku.markedssegment_kode, ku.korrigert_sak_start_dato_flagg, ku.konto_fom_dato,
         ku.konto_tom_dato, ku.beregn_til_dato, ku.beregn_til_tid_id, ku.beregnet_stans_etter_score