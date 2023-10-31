
  create or replace   view RISIKO.LGD.res_sak_bankkonto_kontantstr_t
  
   as (
    
select
t.tid_id,
t.maletidspunkt_kode,
t.kontantstrom_kilde_kode,
t.sk_bankkunde_biii_id,
t.rk_bankkunde_id,
t.rk_bankkonto_id,
t.bk_sb1_selskap_id,
t.sak_start_dato,
t.tilfrisket_dato,
t.kontonummer,
t.eksponering_flagg,
t.kontantstrom_tid_id,
t.kontantstrom_dato,
t.kontantstrom_belop,
t.naverdi_kontantstrom_belop,
t.gyldig_kontantstrom_flagg,
t.gl_belop,
t.gl_korr_henl_saldo_belop,
t.ujust_korr_henl_saldo_belop,
t.gl_korr_over_underkurs_belop,
t.kaptrans_belop,
t.syndikat_deltaker_belop,
t.rentetrekk_belop,
t.cp_kks_exit_belop,
t.nv_gl_belop,
t.nv_gl_korr_henl_saldo_belop,
t.nv_gl_korr_o_underkurs_belop,
t.nv_kaptrans_belop,
t.nv_syndikat_deltakr_belop,
t.nv_rentetrekk_belop,
t.nv_cp_kks_exit_belop,
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
t.rente_navn,
t.rente_ppoeng,
t.rente_referanse_ppoeng,
t.batch_navn
from RISIKO.LGD.m_sak_bankkonto_kontantstrom_t t
join RISIKO.LGD.M_KONFIGURASJON_BANK kb on kb.maletidspunkt_kode = t.maletidspunkt_kode
                                    and kb.bk_sb1_selskap_id = t.bk_sb1_selskap_id
where t.tid_id = '&uttrekksdato'
and t.batch_navn = '&batch_navn'
  );

