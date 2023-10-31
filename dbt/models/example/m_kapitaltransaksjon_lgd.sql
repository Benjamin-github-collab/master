select
/* Del 1: Transaksjoner direkte på kontoer i sak*/
 t.tid_id,
 t.bk_transaksjon_id,
 t.bk_avleverende_system_id,
 t.sk_transaksjonskode_id,
 t.bk_transaksjonskode_id,
 t.rk_bankkonto_id,
 t.bk_sb1_selskap_id,
 t.kontonummer,
 t.transaksjonsbelop_valuta,
 t.transaksjonsbelop_nok,
 t.valutakode,
 t.bokfort_dato,
 t.kapitaltransaksjon_beskrivelse
    from {{ source('LGD_SOURCES', 'F_KAPITALTRANSAKSJON_T') }} t
    join {{ ref('m_d_bankkunde_biii_lgd_t') }} k on t.rk_bankkonto_id = k.rk_bankkonto_id
                                         and t.tid_id between k.sak_start_tid_id and k.beregn_til_tid_id
    join {{ source('LGD_SOURCES', 'M_KONFIGURASJON_BANK') }} kb on kb.maletidspunkt_kode = k.maletidspunkt_kode
                                        and kb.bk_sb1_selskap_id = k.bk_sb1_selskap_id
    /*join sb1_lgd.m_d_bankkunde_biii_kto bk on bk.rk_bankkonto_id = k.rk_bankkonto_id
                                         and t.bokfort_dato <= nvl(bk.konto_tom_dato, bk.beregn_til_dato)  Lagt til for å tilpasse gjenbruk av kontoer*/
    where k.tid_id = '&uttrekksdato'
    and k.batch_navn = '&batch_navn'
union

/* Del 2: Rentetrekk-transaksjoner med knytning til konto i sak*/
select
 t.tid_id,
 t.bk_transaksjon_id,
 t.bk_avleverende_system_id,
 t.sk_transaksjonskode_id,
 t.bk_transaksjonskode_id,
 t.rk_bankkonto_id,
 t.bk_sb1_selskap_id,
 t.kontonummer,
 t.transaksjonsbelop_valuta,
 t.transaksjonsbelop_nok,
 t.valutakode,
 t.bokfort_dato,
 t.kapitaltransaksjon_beskrivelse
  from {{ source('LGD_SOURCES', 'F_KAPITALTRANSAKSJON_T') }} t
  join {{ ref('m_d_bankkunde_biii_lgd_t') }} k2 on k2.kontonummer = replace(substr(regexp_substr(t.kapitaltransaksjon_beskrivelse, 'FRA KTO \d{4}\.\d{2}\.\d{5}'), 9), '.', '')
                                        and t.tid_id between k2.sak_start_tid_id and k2.beregn_til_tid_id

  join {{ source('LGD_SOURCES', 'M_KONFIGURASJON_BANK') }} kb on kb.maletidspunkt_kode = k2.maletidspunkt_kode
                                      and kb.bk_sb1_selskap_id = k2.bk_sb1_selskap_id

  where k2.tid_id = '&uttrekksdato'
  and k2.batch_navn = '&batch_navn'
union

/* Del 3: Deltaker-transaksjoner for syndikatlån*/
select
 t.tid_id,
 t.bk_transaksjon_id,
 t.bk_avleverende_system_id,
 t.sk_transaksjonskode_id,
 t.bk_transaksjonskode_id,
 t.rk_bankkonto_id,
 t.bk_sb1_selskap_id,
 t.kontonummer,
 t.transaksjonsbelop_valuta,
 t.transaksjonsbelop_nok,
 t.valutakode,
 t.bokfort_dato,
 t.kapitaltransaksjon_beskrivelse
  from {{ source('LGD_SOURCES', 'F_KAPITALTRANSAKSJON_T') }} t
  join {{ source('LGD_SOURCES', 'F_KNYTNING_SYNDIKAT') }} ks on ks.rk_bankkonto_deltakerandel_id = t.rk_bankkonto_id
                                     and ks.tid_id = t.tid_id
  join {{ ref('m_d_bankkunde_biii_lgd_t') }} k3 on k3.rk_bankkonto_id = ks.rk_bankkonto_hovedandel_id
                                        and t.tid_id between k3.sak_start_tid_id and k3.beregn_til_tid_id
  join {{ source('LGD_SOURCES', 'M_KONFIGURASJON_BANK') }} kb on kb.maletidspunkt_kode = k3.maletidspunkt_kode
                                      and kb.bk_sb1_selskap_id = k3.bk_sb1_selskap_id

 where k3.tid_id = '&uttrekksdato'
 and k3.batch_navn = '&batch_navn'                                        