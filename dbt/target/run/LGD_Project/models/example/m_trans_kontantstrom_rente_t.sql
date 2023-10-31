
  create or replace   view RISIKO.LGD.m_trans_kontantstrom_rente_t
  
   as (
    select '&uttrekksdato' as tid_id,
       k.maletidspunkt_kode,
       tid.dato,
       k.sk_bankkunde_biii_id,
       k.rk_bankkonto_id,
       k.bk_sb1_selskap_id,
       k.kontonummer,
       -sum(t.transaksjonsbelop_nok) as gjenvinning_belop,
       '&batch_navn' as batch_navn
  from RISIKO.LGD.m_d_bankkunde_biii_lgd_t k 
  join RISIKO.LGD.M_KONFIGURASJON_BANK kb on kb.maletidspunkt_kode = k.maletidspunkt_kode
                                      and kb.bk_sb1_selskap_id = k.bk_sb1_selskap_id
  join RISIKO.LGD.m_kapitaltransaksjon_lgd t on k.kontonummer =
                                               replace(substr(regexp_substr(t.kapitaltransaksjon_beskrivelse, 'FRA KTO \d{4}\.\d{2}\.\d{5}'), 9),
                                                       '.',
                                                       '')
                                           and t.tid_id between k.sak_start_tid_id and k.beregn_til_tid_id
  join RISIKO.LGD.D_TID tid on t.tid_id = tid.tid_id
  left join RISIKO.LGD.v_cashpool_ikke_balanse cash on cash.bk_bankkonto_id = t.kontonummer
                                                and tid.dato between cash.scd_gyldig_fom and cash.scd_gyldig_tom
 where k.tid_id = '&uttrekksdato'
   and k.batch_navn = '&batch_navn'
   and cash.bk_bankkonto_id is null
   and t.bk_transaksjonskode_id in ('R_744', 'R_745', 'R_746')
   and t.transaksjonsbelop_nok < 0
 group by t.tid_id, k.maletidspunkt_kode, tid.dato, k.sk_bankkunde_biii_id, k.rk_bankkonto_id, k.bk_sb1_selskap_id, k.kontonummer
  );

