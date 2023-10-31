
  create or replace   view RISIKO.LGD.res_sak_bankkunde_lgd_t
  
   as (
    with sak_bankkonto as 
    (select sk_bankkunde_biii_id, kontonummer, valutakode, rank() over(partition by sk_bankkunde_biii_id order by kontonummer desc) rnk
     from (
           select distinct sk_bankkunde_biii_id, kontonummer, t.valutakode 
           from RISIKO.LGD.m_sak_bankkonto_lgd_t t
           join RISIKO.LGD.M_KONFIGURASJON_BANK kb on kb.maletidspunkt_kode = t.maletidspunkt_kode
                                               and kb.bk_sb1_selskap_id = t.bk_sb1_selskap_id
           where t.ekskludert_konto_flagg = '0'
             and t.tid_id = '&uttrekksdato'
             and t.batch_navn = '&batch_navn'
          )
    )
select
       t.tid_id,t.maletidspunkt_kode,t.kontantstrom_kilde_kode,t.sk_bankkunde_biii_id,t.rk_bankkunde_id,t.bk_sb1_selskap_id,t.kundenummer,t.kundenavn,t.overforing_arsak_init_kode,t.overforing_arsak_oppdat_kode,t.sak_start_dato,
       t.tilfrisket_dato,
       max(t.beregn_til_dato) max_kto_beregn_til_dato,
       uk.kontonummer_liste,
       sum(t.mislighold_ead_total_daglast) mislighold_ead_total_daglast,
       sum(t.mislighold_saldo_renter_belop) mislighold_saldo_renter_belop,
       sum(t.naverdi_gjenvunnet_belop) naverdi_gjenvunnet_belop,
       sum(t.naverdi_tid_saldo_renter_belop) naverdi_tid_saldo_renter_belop,
       min(t.konstatert_tap_dato) min_konstatert_tap_dato,
       sum(t.konstatert_tap_belop) sum_konstatert_tap_belop,
       sum(case when t.konstatert_tap_dato is not null then 1 else 0 end) antall_kto_med_konstatert_tap,
       max(t.konstatert_tap_dato) max_konstatert_tap_dato,
       sum(t.naverdi_gjenvunnet_kt_belop) naverdi_gjenvunnet_kt_belop,
       sum(t.mislighold_vintage_irba_ead) mislighold_vintage_irba_ead,
       sum(t.mislighold_vintage_irba_sikk) mislighold_vintage_irba_sikk,
       sum(t.gjenvunnet_nominelt_belop) gjenvunnet_nominelt_belop,
       sum(t.gjenvunnet_nominelt_kt_belop) gjenvunnet_nominelt_kt_belop,
       flagg.kredittforetak_flagg,flagg.syndikat_flagg,flagg.eierbytte_flagg,flagg.rk_trekkonto_utenfor_lgd_flagg,flagg.korr_kilde_trans_flagg,flagg.korr_kilde_gl_flagg,flagg.korr_kilde_konflikt_flagg,flagg.ekskludert_konto_flagg,/*flagg.annet_spesielt,*/
       sum(least(t.mislighold_saldo_nok, 0)) negativ_mislighold_saldo_nok,
       vk.valutakode_liste,
       sum(t.misl_ikkekap_kreditrente_belop) misl_ikkekap_kreditrente_belop,
       sum(t.misl_ikkekap_debetrente_belop) misl_ikkekap_debetrente_belop,
       sum(t.tid_saldo_renter_belop) tid_saldo_renter_belop,
       t.rente_navn,t.rente_ppoeng,t.rente_referanse_ppoeng,t.sak_start_tid_id,t.sak_start_dato_biii,t.sak_start_dato_biii_siste,t.sak_kilde_init,t.sak_kilde_oppdatert,
       t.tilfrisket_tid_id,
       case when t.tilfrisket_dato <= max(t.beregn_til_dato) then '1' else '0' end tilfrisket_flagg,
       t.tilfrisket_senere_flagg,t.kundesak_antall_9mnd,t.historisk_realisasjon_flagg,t.saker_i_sak_antall,t.markedssegment_kode,t.korrigert_sak_start_dato_flagg,
       '&batch_navn' as batch_navn
  from RISIKO.LGD.m_sak_bankkonto_lgd_t t /* materialisert tabell iht. sb1_lgd.p_lgd_last*/
  join RISIKO.LGD.P_LGD_M_KONFIGURASJON kb on kb.maletidspunkt_kode = t.maletidspunkt_kode
  join RISIKO.LGD.m_d_bankkunde_biii_flagg_t flagg on flagg.tid_id = '&uttrekksdato'
                                               and flagg.batch_navn = '&batch_navn'
                                               and flagg.sk_bankkunde_biii_id = t.sk_bankkunde_biii_id
                                               and flagg.maletidspunkt_kode = t.maletidspunkt_kode

/*utvalgte_konti*/
  join (select sk_bankkunde_biii_id, listagg(kontonummer, ', ') within group(order by kontonummer desc) kontonummer_liste,
               listagg(valutakode, ', ') within group(order by valutakode desc) valutakode_liste
        from sak_bankkonto
        where rnk <= 30 /*Antall konti som skal med i listen*/
        group by sk_bankkunde_biii_id
       ) uk on uk.sk_bankkunde_biii_id = t.sk_bankkunde_biii_id

  join (select sk_bankkunde_biii_id, listagg(valutakode, ', ') within group(order by valutakode desc) valutakode_liste
        from 
           (select distinct sk_bankkunde_biii_id, valutakode
            from sak_bankkonto
           )
        group by sk_bankkunde_biii_id
       ) vk on vk.sk_bankkunde_biii_id = t.sk_bankkunde_biii_id
 where t.ekskludert_konto_flagg = '0'
   and t.tid_id = '&uttrekksdato'
   and t.batch_navn ='&batch_navn'
 group by t.tid_id,t.maletidspunkt_kode,t.kontantstrom_kilde_kode,t.sk_bankkunde_biii_id,t.rk_bankkunde_id,t.bk_sb1_selskap_id,t.kundenummer,t.kundenavn,t.overforing_arsak_init_kode,t.overforing_arsak_oppdat_kode,t.sak_start_dato,t.tilfrisket_dato,uk.kontonummer_liste,vk.valutakode_liste,flagg.kredittforetak_flagg,flagg.syndikat_flagg,flagg.eierbytte_flagg,flagg.rk_trekkonto_utenfor_lgd_flagg,flagg.korr_kilde_trans_flagg,flagg.korr_kilde_gl_flagg,flagg.korr_kilde_konflikt_flagg,flagg.ekskludert_konto_flagg,/*flagg.annet_spesielt,*/t.rente_navn,t.rente_ppoeng,t.rente_referanse_ppoeng,t.sak_start_tid_id,t.sak_start_dato_biii,t.sak_start_dato_biii_siste,t.sak_kilde_init,t.sak_kilde_oppdatert,t.tilfrisket_tid_id,t.tilfrisket_flagg,t.tilfrisket_senere_flagg,t.kundesak_antall_9mnd,t.historisk_realisasjon_flagg,t.saker_i_sak_antall,t.markedssegment_kode,t.korrigert_sak_start_dato_flagg
  );

