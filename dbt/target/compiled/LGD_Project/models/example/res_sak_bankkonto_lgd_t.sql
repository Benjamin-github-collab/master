select
 t.tid_id,
 t.maletidspunkt_kode,
 t.kontantstrom_kilde_kode,
 t.sk_bankkunde_biii_id,
 t.rk_bankkunde_id,
 t.rk_bankkonto_id,
 t.bk_sb1_selskap_id,
 t.kundenummer,
 t.kundenavn,
 t.overforing_arsak_init_kode,
 t.overforing_arsak_oppdat_kode,
 t.sak_start_dato,
 t.tilfrisket_dato,
 t.kontonummer,
 t.mislighold_ead_total_daglast,
 t.mislighold_saldo_renter_belop,
 t.naverdi_gjenvunnet_belop,
 t.naverdi_tid_saldo_renter_belop,
 greatest(t.mislighold_saldo_renter_belop - t.naverdi_gjenvunnet_belop - t.naverdi_tid_saldo_renter_belop, 0) brutto_observert_lgd_belop,
 round(case when ku.observert_lgd_belop = 0 then 0
            when nvl(sum(l.lgd_belop_konto) over (partition by t.tid_id, t.maletidspunkt_kode, t.sk_bankkunde_biii_id), 0) > 0
              then ku.observert_lgd_belop * nvl(l.lgd_belop_konto, 0) / nvl(sum(l.lgd_belop_konto) over (partition by t.tid_id, t.maletidspunkt_kode, t.sk_bankkunde_biii_id), 0)
            when ku.mislighold_saldo_renter_belop > 0
              then ku.observert_lgd_belop * t.mislighold_saldo_renter_belop / sum(t.mislighold_saldo_renter_belop) over (partition by t.tid_id, t.maletidspunkt_kode, t.sk_bankkunde_biii_id)
            else ku.observert_lgd_belop * greatest(t.mislighold_saldo_renter_belop - t.naverdi_gjenvunnet_belop - t.naverdi_tid_saldo_renter_belop, 0)
                   / sum(greatest(t.mislighold_saldo_renter_belop - t.naverdi_gjenvunnet_belop - t.naverdi_tid_saldo_renter_belop, 0)) over (partition by t.tid_id, t.maletidspunkt_kode, t.sk_bankkunde_biii_id)
        end, 2) fordelt_observert_lgd_belop,
 t.konstatert_tap_dato,
 t.konstatert_tap_belop,
 t.naverdi_gjenvunnet_kt_belop,
 t.mislighold_vintage_irba_ead,
 t.mislighold_vintage_irba_sikk,
 l.lgd_belop_konto misl_vintage_irba_lgd_belop,
 l.lgd_konto misl_vintage_irba_lgd_faktor,
 t.gjenvunnet_nominelt_belop,
 t.gjenvunnet_nominelt_kt_belop,
 t.eksponering_flagg,
 t.kredittforetak_flagg,
 t.syndikat_flagg,
 t.eierbytte_flagg,
 t.rk_trekkonto_utenfor_lgd_flagg,
 t.korr_kilde_trans_flagg,
 t.korr_kilde_gl_flagg,
 t.korr_kilde_konflikt_flagg,
 t.ekskludert_konto_flagg,
 t.ekskludert_konto_arsak,
 t.annet_spesielt,
 t.mislighold_saldo_nok,
 t.valutakode,
 t.misl_ikkekap_kreditrente_belop,
 t.misl_ikkekap_debetrente_belop,
 t.tid_saldo_renter_belop,
 t.cp_mis_ikke_balansekonto_flagg,
 t.kks_mis_underkonto_flagg,
 t.cp_tid_ikke_balansekonto_flagg,
 t.kks_tid_underkonto_flagg,
 t.rente_navn,
 t.rente_ppoeng,
 t.rente_referanse_ppoeng,
 t.sak_start_tid_id,
 t.sak_start_dato_biii,
 t.sak_start_dato_biii_siste,
 t.sak_kilde_init,
 t.sak_kilde_oppdatert,
 t.tilfrisket_tid_id,
 t.tilfrisket_flagg,
 t.tilfrisket_senere_flagg,
 t.kundesak_antall_9mnd,
 t.historisk_realisasjon_flagg,
 t.saker_i_sak_antall,
 t.markedssegment_kode,
 t.korrigert_sak_start_dato_flagg,
 t.konto_fom_dato,
 t.konto_tom_dato,
 t.beregn_til_dato,
 t.beregn_til_tid_id,
 t.beregnet_stans_etter_score,
 '&batch_navn' as batch_navn
  from RISIKO.LGD.m_sak_bankkonto_lgd_t t
  join RISIKO.LGD.M_KONFIGURASJON_BANK kb on kb.maletidspunkt_kode = t.maletidspunkt_kode
                                      and kb.bk_sb1_selskap_id = t.bk_sb1_selskap_id
  join RISIKO.LGD.res_sak_bankkunde_lgd_t  ku on ku.tid_id = '&uttrekksdato'
                                         and ku.batch_navn = '&batch_navn'
                                         and ku.maletidspunkt_kode = t.maletidspunkt_kode
                                         and ku.sk_bankkunde_biii_id = t.sk_bankkunde_biii_id
  join RISIKO.LGD.D_TID mis_tid on mis_tid.tid_id = t.sak_start_tid_id
  left join RISIKO.LGD.P_LGD_BATCH p_mis_lgd on p_mis_lgd.lgd_flagg = '1'
                                         and substr(mis_tid.tid_id_depot, 1, 6) between nvl(p_mis_lgd.vintage_irba_fom, '000000') and nvl(p_mis_lgd.vintage_irba_tom, '999999')
                                         and nvl(p_mis_lgd.vintage_irba_fom, p_mis_lgd.vintage_irba_tom) is not null
  left join RISIKO.LGD.F_LGD_T l on l.tid_id = mis_tid.tid_id_depot
                             and l.rk_bankkonto_id = t.rk_bankkonto_id
                             and l.batch_navn = p_mis_lgd.batch_navn
where t.tid_id = '&uttrekksdato'
and t.batch_navn = '&batch_navn'