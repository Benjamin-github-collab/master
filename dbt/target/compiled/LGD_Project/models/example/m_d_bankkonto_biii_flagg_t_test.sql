
with
eksponering as (
  select /*+ MATERIALIZE */ ku.sk_bankkunde_biii_id, ku.rk_bankkonto_id, ku.kontonummer,
         max(case when eksp.ead_total > 0 then '1' else '0' end) eksponering_flagg
    from RISIKO.LGD.m_d_bankkunde_biii_lgd_t ku
    join RISIKO.LGD.M_KONFIGURASJON_BANK kb on kb.bk_sb1_selskap_id = ku.bk_sb1_selskap_id
                                        and kb.maletidspunkt_kode = ku.maletidspunkt_kode
    join RISIKO.LGD.D_VIRKEDAG tid_start on ku.sak_start_tid_id  = tid_start.tid_id
    left join RISIKO.LGD.F_EAD_T eksp on eksp.tid_id between tid_start.forrige_virkedag_tid_id and least(ku.beregn_til_tid_id, ku.tilfrisket_tid_id, nvl(ku.konto_tom_tid_id, ku.tilfrisket_tid_id), ku.tid_id)
                                and case when eksp.ead_total > 0 then eksp.rk_bankkonto_id else null end = ku.rk_bankkonto_id
  where ku.tid_id = '20230331'
    and ku.batch_navn = 'batch_navn'                                
   group by ku.sk_bankkunde_biii_id, ku.rk_bankkonto_id, ku.kontonummer
),

kredittforetak as (
  select /*+ MATERIALIZE */ ku.sk_bankkunde_biii_id, ku.rk_bankkonto_id, ku.kontonummer,
         max(case when ko.rk_bankkonto_id is not null then '1' else '0' end) kredittforetak_flagg
    from RISIKO.LGD.m_d_bankkunde_biii_lgd_t ku
    join RISIKO.LGD.M_KONFIGURASJON_BANK kb on kb.bk_sb1_selskap_id = ku.bk_sb1_selskap_id
                                        and kb.maletidspunkt_kode = ku.maletidspunkt_kode

    left join RISIKO.LGD.FAKE_D_BANKKONTO ko on ku.rk_bankkonto_id = ko.rk_bankkonto_id
                                         and ko.scd_gyldig_fom <= ku.tid_dato
                                         /* Inkluderer også kontroll mot dagen før sak_start_dato, fordi en tibakeføring til bankene fra og med sak_start_dato vil forårsake rot i regnskapet*/
                                         and ko.scd_gyldig_tom >= ku.sak_start_dato - 1
                                         and ko.bk_sb1_selskap_forvalter_id <> ko.bk_sb1_selskap_eier_id
   where ku.tid_id = '20230331'
   and ku.batch_navn = 'batch_navn'
   group by ku.sk_bankkunde_biii_id, ku.rk_bankkonto_id, ku.kontonummer
),

syndikat as (
  select /*+ MATERIALIZE */ ku.sk_bankkunde_biii_id, ku.rk_bankkonto_id, ku.kontonummer,
         max(case when s.kontonummer_deltakerandel is not null then '1' else '0' end) syndikat_flagg
    from RISIKO.LGD.m_d_bankkunde_biii_lgd_t ku
    join RISIKO.LGD.M_KONFIGURASJON_BANK kb on kb.bk_sb1_selskap_id = ku.bk_sb1_selskap_id
                                        and kb.maletidspunkt_kode = ku.maletidspunkt_kode

    left join RISIKO.LGD.F_KNYTNING_SYNDIKAT s on s.tid_id between ku.sak_start_tid_id and ku.tid_id
                                           and s.kontonummer_hovedandel = ku.kontonummer
   where ku.tid_id = '20230331'
   and ku.batch_navn = 'batch_navn'
   group by ku.sk_bankkunde_biii_id, ku.rk_bankkonto_id, ku.kontonummer
)
select * from syndikat