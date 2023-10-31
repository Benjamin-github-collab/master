/**********************************************************************************************
Beskrivelse: View som genererer opp relevante flagg for beregning av LGD per konto.
             For at ikke saker skal vingle mellom beregninger basert på måletidspunktet,
             så beregnes flagg alltid per måletidspunkt tid_id.

Tabellgrunnlag:  m_d_bankkunde_biii_lgd
                 f_ead
                 d_tid
                 d_bankkonto_biii
                 f_knytning_syndikat
                 f_kontobalanse
                 d_reskontro_trekkonto
                 p_korr_trans_kilde_kode
                 f_kontobalanse_t

Endringslogg:
Initialier   Dato         Beskrivelse
MBJ          15.12.20     Opprettet view
MJ           19.01.23     Overført til tabellstyrt tool-entilen

***********************************************************************************************/
{{ config(materialized='table') }}
with
eksponering as (
  select /*+ MATERIALIZE */ ku.sk_bankkunde_biii_id, ku.rk_bankkonto_id, ku.kontonummer,
         max(case when eksp.ead_total > 0 then '1' else '0' end) eksponering_flagg
    from {{ ref('m_d_bankkunde_biii_lgd_t') }} ku
    join {{ source('LGD_SOURCES', 'M_KONFIGURASJON_BANK') }} kb on kb.bk_sb1_selskap_id = ku.bk_sb1_selskap_id
                                        and kb.maletidspunkt_kode = ku.maletidspunkt_kode
    join {{ ref('d_virkedag') }} tid_start on ku.sak_start_tid_id  = tid_start.tid_id
    left join {{ source('LGD_SOURCES', 'F_EAD_T') }} eksp on eksp.tid_id between tid_start.forrige_virkedag_tid_id and least(ku.beregn_til_tid_id, ku.tilfrisket_tid_id, nvl(ku.konto_tom_tid_id, ku.tilfrisket_tid_id), ku.tid_id)
                                and case when eksp.ead_total > 0 then eksp.rk_bankkonto_id else null end = ku.rk_bankkonto_id
  where ku.tid_id = '20230331'
    and ku.batch_navn = 'batch_navn'                                
   group by ku.sk_bankkunde_biii_id, ku.rk_bankkonto_id, ku.kontonummer
),
kredittforetak as (
  select /*+ MATERIALIZE */ ku.sk_bankkunde_biii_id, ku.rk_bankkonto_id, ku.kontonummer,
         max(case when ko.rk_bankkonto_id is not null then '1' else '0' end) kredittforetak_flagg
    from {{ ref('m_d_bankkunde_biii_lgd_t') }} ku
    join {{ source('LGD_SOURCES', 'M_KONFIGURASJON_BANK') }} kb on kb.bk_sb1_selskap_id = ku.bk_sb1_selskap_id
                                        and kb.maletidspunkt_kode = ku.maletidspunkt_kode

    left join {{ source('LGD_SOURCES', 'FAKE_D_BANKKONTO') }} ko on ku.rk_bankkonto_id = ko.rk_bankkonto_id
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
    from {{ ref('m_d_bankkunde_biii_lgd_t') }} ku
    join {{ source('LGD_SOURCES', 'M_KONFIGURASJON_BANK') }} kb on kb.bk_sb1_selskap_id = ku.bk_sb1_selskap_id
                                        and kb.maletidspunkt_kode = ku.maletidspunkt_kode

    left join {{ source('LGD_SOURCES', 'F_KNYTNING_SYNDIKAT') }} s on s.tid_id between ku.sak_start_tid_id and ku.tid_id
                                           and s.kontonummer_hovedandel = ku.kontonummer
   where ku.tid_id = '20230331'
   and ku.batch_navn = 'batch_navn'
   group by ku.sk_bankkunde_biii_id, ku.rk_bankkonto_id, ku.kontonummer
),
rk_trekkonto_utenfor_lgd as (
  select /*+ MATERIALIZE */ ku.sk_bankkunde_biii_id, ku.rk_bankkonto_id, ku.kontonummer,
         max(case when rkt.kontonummer is not null and rkt_ak.kontonummer is null then '1' else '0' end) rk_trekkonto_utenfor_lgd_flagg
    from {{ ref('m_d_bankkunde_biii_lgd_t') }} ku
    join {{ source('LGD_SOURCES', 'M_KONFIGURASJON_BANK') }} kb on kb.bk_sb1_selskap_id = ku.bk_sb1_selskap_id
                                        and kb.maletidspunkt_kode = ku.maletidspunkt_kode

    left join {{ source('LGD_SOURCES', 'D_RESKONTRO_TREKKONTO') }} rkt on rkt.kontonummer = ku.kontonummer
                                               and rkt.scd_gyldig_tom >= ku.sak_start_dato
                                               and rkt.scd_gyldig_fom <= ku.beregn_til_dato
    left join {{ source('LGD_SOURCES', 'FAKE_D_BANKKONTO') }} rkt_ak on rkt_ak.kontonummer = rkt.kontonummer_trekkonto
   where ku.tid_id = '20230331'
   and ku.batch_navn = 'batch_navn'

   group by ku.sk_bankkunde_biii_id, ku.rk_bankkonto_id, ku.kontonummer
),
korr_trans_kilde as (
  select /*+ MATERIALIZE */ ku.sk_bankkunde_biii_id, ku.rk_bankkonto_id, ku.kontonummer,
         max(case when p.trans_kilde_kode = 'TRANS' then '1' else '0' end) korr_kilde_trans_flagg,
         max(case when p.trans_kilde_kode = 'GL' then '1' else '0' end) korr_kilde_gl_flagg
    from {{ ref('m_d_bankkunde_biii_lgd_t') }} ku
    join {{ source('LGD_SOURCES', 'M_KONFIGURASJON_BANK') }} kb on kb.bk_sb1_selskap_id = ku.bk_sb1_selskap_id
                                        and kb.maletidspunkt_kode = ku.maletidspunkt_kode
    
    left join {{ source('LGD_SOURCES', 'P_KORR_TRANS_KILDE_KODE') }} p on p.rk_bankkunde_id = ku.rk_bankkunde_id

   where ku.tid_id = '20230331'
   and ku.batch_navn = 'batch_navn'
   group by ku.sk_bankkunde_biii_id, ku.rk_bankkonto_id, ku.kontonummer
),
smn_bal_mangler_20180108 as (
  select /*+ MATERIALIZE */ ku.sk_bankkunde_biii_id, ku.rk_bankkonto_id, ku.kontonummer,
         case when FIRST_VALUE(ku.bk_sb1_selskap_id) OVER (ORDER BY 1) = '4210'
         --case when max(ku.bk_sb1_selskap_id) keep (dense_rank first order by 1) = '4210'
                   and max(case when bal_mangler.tid_id in ('20180105', '20180109') then '1' else '0' end) = '1'
                   and max(case when bal_mangler.tid_id in ('20180108') then '1' else '0' end) = '0'
                   and max(case when bal_mangler.tid_id in ('20180109') then bal_mangler.saldo_valuta else null end) - max(case when bal_mangler.tid_id in ('20180105') then bal_mangler.saldo_valuta else null end) <> 0
                   and (max(case when bal_mangler.tid_id in ('20180109') then bal_mangler.saldo_valuta else null end) < 0
                        or max(case when bal_mangler.tid_id in ('20180105') then bal_mangler.saldo_valuta else null end) < 0)
                then '1:SMN kontobalansemangel 20180108' else '' end spesielt
    from {{ ref('m_d_bankkunde_biii_lgd_t') }} ku
    join {{ source('LGD_SOURCES', 'M_KONFIGURASJON_BANK') }} kb on kb.bk_sb1_selskap_id = ku.bk_sb1_selskap_id
                                        and kb.maletidspunkt_kode = ku.maletidspunkt_kode
    
    left join {{ source('LGD_SOURCES', 'F_KONTOBALANSE_T') }} bal_mangler on bal_mangler.tid_id between '20180105' and '20180109'
                                                      and bal_mangler.tid_id between ku.sak_start_tid_id and ku.beregn_til_tid_id
                                                      and bal_mangler.rk_bankkonto_id = ku.rk_bankkonto_id
   where ku.tid_id = '20230331'
   and ku.batch_navn = 'batch_navn'
   group by ku.sk_bankkunde_biii_id, ku.rk_bankkonto_id, ku.kontonummer, ku.bk_sb1_selskap_id
),
ul_trans_mangler_20190826 as (
  select /*+ MATERIALIZE */ ku.sk_bankkunde_biii_id, ku.rk_bankkonto_id, ku.kontonummer,
         case when FIRST_VALUE(ko.edb_rutine) OVER (ORDER BY 1) = 'U'
         --case when max(ko.edb_rutine) keep (dense_rank first order by 1) = 'U'
                   and max(case when bal_mangler.tid_id in ('20190823', '20190827') then '1' else '0' end) = '1'
                   and max(case when bal_mangler.tid_id in ('20190826') then '1' else '0' end) = '0'
                   and max(case when bal_mangler.tid_id in ('20190827') then bal_mangler.saldo_valuta else null end) - max(case when bal_mangler.tid_id in ('20190823') then bal_mangler.saldo_valuta else null end) <> 0
                   and (max(case when bal_mangler.tid_id in ('20190827') then bal_mangler.saldo_valuta else null end) < 0
                        or max(case when bal_mangler.tid_id in ('20190823') then bal_mangler.saldo_valuta else null end) < 0)
                then '2:UL-kapitaltransaksjoner mangler 20190826' else '' end spesielt
    from {{ ref('m_d_bankkunde_biii_lgd_t') }} ku
    join {{ source('LGD_SOURCES', 'M_KONFIGURASJON_BANK') }} kb on kb.bk_sb1_selskap_id = ku.bk_sb1_selskap_id
                                        and kb.maletidspunkt_kode = ku.maletidspunkt_kode
    
    left join {{ source('LGD_SOURCES', 'FAKE_D_BANKKONTO') }} ko on ku.rk_bankkonto_id = ko.rk_bankkonto_id
                                    and ku.beregn_til_dato between ko.scd_gyldig_fom and ko.scd_gyldig_tom
    left join {{ source('LGD_SOURCES', 'F_KONTOBALANSE_T') }} bal_mangler on bal_mangler.tid_id between '20190823' and '20190827'
                                                  and bal_mangler.tid_id between ku.sak_start_tid_id and ku.beregn_til_tid_id
                                                  and bal_mangler.rk_bankkonto_id = ku.rk_bankkonto_id
   where ku.tid_id = '20230331'
   and ku.batch_navn = 'batch_navn'
   group by ku.sk_bankkunde_biii_id, ku.rk_bankkonto_id, ku.kontonummer, ko.edb_rutine
),
ekskludert_konto as (
  select /*+ MATERIALIZE */ ku.sk_bankkunde_biii_id, ku.rk_bankkonto_id, ku.kontonummer,
         max(ku.ekskludert_konto_flagg) ekskludert_konto_flagg,
         listagg(case when ku.ekskludert_konto_arsak is not null then replace(ku.ekskludert_konto_arsak, ',', '') end, ',') within group (order by ku.ekskludert_konto_arsak) ekskludert_konto_arsak
    from (select distinct ku.sk_bankkunde_biii_id, ku.rk_bankkonto_id, ku.kontonummer,
             case
                   /*Ekskluderte forbrukslån for BN Bank (avsluttet produkt og nær avsluttet portefølje, beregnes feil)*/
               when ku.bk_sb1_selskap_id = '9236' and ko.ko_kode in ('701002', '621912') then '1'
               else '0'
              end ekskludert_konto_flagg,
             case /* Ekskluderte forbrukslån for BN Bank (avsluttet produkt og nær avsluttet portefølje, beregnes feil)*/
               when ku.bk_sb1_selskap_id = '9236' and ko.ko_kode in ('701002', '621912') then 'Forbrukslån (KO-kode ' || ko.ko_kode || ')'
              end ekskludert_konto_arsak
    from {{ ref('m_d_bankkunde_biii_lgd_t') }} ku
    join {{ source('LGD_SOURCES', 'M_KONFIGURASJON_BANK') }} kb on kb.bk_sb1_selskap_id = ku.bk_sb1_selskap_id
                                        and kb.maletidspunkt_kode = ku.maletidspunkt_kode
    
    left join {{ source('LGD_SOURCES', 'FAKE_D_BANKKONTO') }} ko on ku.rk_bankkonto_id = ko.rk_bankkonto_id
    where ku.tid_id = '20230331'
      and ku.batch_navn = 'batch_navn'
    ) ku
   group by ku.sk_bankkunde_biii_id, ku.rk_bankkonto_id, ku.kontonummer
),
annet_spesielt as (
  select /*+ MATERIALIZE */ sk_bankkunde_biii_id, rk_bankkonto_id, kontonummer,
         listagg(case when spesielt is not null then replace(spesielt, ',', '') else null end, ',') within group (order by spesielt) annet_spesielt
    from (select * from smn_bal_mangler_20180108
          union all
          select * from ul_trans_mangler_20190826)
   group by sk_bankkunde_biii_id, rk_bankkonto_id, kontonummer
)
select ku.tid_id,
       ku.sk_bankkunde_biii_id,
       kb.maletidspunkt_kode,
       ku.bk_sb1_selskap_id,
       ku.sak_start_dato,
       ku.beregn_til_dato,
       ku.rk_bankkonto_id,
       ku.kontonummer,
       e.eksponering_flagg,
       k.kredittforetak_flagg,
       s.syndikat_flagg,
       ku.annen_eier_i_lgd_db_flagg eierbytte_flagg,
       r.rk_trekkonto_utenfor_lgd_flagg,
       ktk.korr_kilde_trans_flagg,
       ktk.korr_kilde_gl_flagg,
       ek.ekskludert_konto_flagg,
       ek.ekskludert_konto_arsak,
       sp.annet_spesielt,
       'batch_navn' as batch_navn
  from {{ ref('m_d_bankkunde_biii_lgd_t') }} ku 
  join {{ source('LGD_SOURCES', 'M_KONFIGURASJON_BANK') }} kb on kb.bk_sb1_selskap_id = ku.bk_sb1_selskap_id
                                      and kb.maletidspunkt_kode = ku.maletidspunkt_kode
  join eksponering e on e.sk_bankkunde_biii_id = ku.sk_bankkunde_biii_id
                    and e.rk_bankkonto_id = ku.rk_bankkonto_id
  join kredittforetak k on k.sk_bankkunde_biii_id = ku.sk_bankkunde_biii_id
                       and k.rk_bankkonto_id = ku.rk_bankkonto_id
  join syndikat s on s.sk_bankkunde_biii_id = ku.sk_bankkunde_biii_id
                 and s.rk_bankkonto_id = ku.rk_bankkonto_id
  join rk_trekkonto_utenfor_lgd r on r.sk_bankkunde_biii_id = ku.sk_bankkunde_biii_id
                                 and r.rk_bankkonto_id = ku.rk_bankkonto_id
  join korr_trans_kilde ktk on ktk.sk_bankkunde_biii_id = ku.sk_bankkunde_biii_id
                           and ktk.rk_bankkonto_id = ku.rk_bankkonto_id
  join ekskludert_konto ek on ek.sk_bankkunde_biii_id = ku.sk_bankkunde_biii_id
                          and ek.rk_bankkonto_id = ku.rk_bankkonto_id
  join annet_spesielt sp on sp.sk_bankkunde_biii_id = ku.sk_bankkunde_biii_id
                        and sp.rk_bankkonto_id = ku.rk_bankkonto_id
where ku.tid_id = '20230331'
and ku.batch_navn = 'batch_navn' 