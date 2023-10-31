{{ config(materialized = 'view') }}
with
/**********************************************************************************************
Beskrivelse: Viewet identifiserer opptrekk som følge av at konto er tatt ut av en
             konsernkontostruktur eller et cashpool-hierarki.

Tabellgrunnlag: m_d_bankkunde_biii_lgd_t
                d_cashpoolhierarki
                d_reskontro
                f_kontobalanse_t
                d_tid

Endringslogg:
Initialier   Dato         Beskrivelse
MBJ          16.12.20     Opprettet view
MJ           25.01.23     Endret frs å slå opp mot m_d_bankkunde_biii_lgd til å slå opp mot m_d_bankkunde_biii_lgd_t samt lagt til filter på tid og batch
***********************************************************************************************/
tid as (
  select tid_id, dato, lag(tid_id) over (partition by 1 order by tid_id) tid_forrige_virkedag_id from {{ source('LGD_SOURCES', 'D_TID') }} where virkedag_flagg = '1'
),
cp_exit as (
  select c.bk_bankkonto_id, c.scd_slettet_i_kilde_dato
    from {{ source('LGD_SOURCES', 'D_CASHPOOLHIERARKI') }} c
   where c.scd_slettet_i_kilde_dato = c.scd_gyldig_fom
     and c.balansekonto_sb1_flagg = '0'
     and c.cashpoolkonto_status = 'LEAVING'
),
kks as (
select rk.bk_bankkonto_id, rk.scd_gyldig_fom, rk.scd_gyldig_tom
  from (select rk.rk_bankkonto_id,
               rk.bk_bankkonto_id,
               rk.kks_kode,
               rk.kks_hovedkonto_nummer,
               rk.kks_eierkonto_nummer,
               rk.kks_konto_rolle_kode,
               rk.kks_konto_type_kode,
               lag(rk.kks_hovedkonto_nummer) over (partition by rk.bk_bankkonto_id order by rk.scd_gyldig_fom) lag_kks_hovedkonto_nummer,
               lag(rk.kks_konto_type_kode) over (partition by rk.bk_bankkonto_id order by rk.scd_gyldig_fom) lag_kks_konto_type_kode,
               rk.scd_gyldig_fom,
               rk.scd_gyldig_tom,
               rk.scd_aktiv_flagg
          from {{ source('LGD_SOURCES', 'D_RESKONTRO') }} rk) rk
 where nvl(nullif(rk.kks_hovedkonto_nummer, '00000000000'), rk.bk_bankkonto_id) = rk.bk_bankkonto_id -- Ikke KKS eller ER hovedkonto
   and nvl(nullif(rk.lag_kks_hovedkonto_nummer, '00000000000'), rk.bk_bankkonto_id) <> rk.bk_bankkonto_id -- KKS hovedkonto forrige rad må være ulik bk_bankkonto_id, null og '000..' gir ikke ulikhet
   and nvl(rk.lag_kks_konto_type_kode, 'X') = 'KOVF' -- KKS konto type forrige rad må være KOVF
)
select tid.tid_id,
       tid.dato,
       k.sk_bankkunde_biii_id,
       k.rk_bankkonto_id,
       k.bk_sb1_selskap_id,
       k.kontonummer,
       least(b.saldo_nok + nvl(b.ikkekap_kreditrente_belop, 0) + nvl(b.ikkekap_debetrente_belop, 0), 0) as gjenvinning_belop
  from {{ ref('m_d_bankkunde_biii_lgd_t') }} k -- materialisert tabell iht. sb1_lgd.p_lgd_last
  left join cp_exit c on c.bk_bankkonto_id = k.kontonummer
                     and c.scd_slettet_i_kilde_dato between k.sak_start_dato and k.beregn_til_dato
  join tid tid on tid.dato = c.scd_slettet_i_kilde_dato
  join {{ source('LGD_SOURCES', 'F_KONTOBALANSE_T') }} b on b.tid_id = tid_forrige_virkedag_id
                                 and b.rk_bankkonto_id = k.rk_bankkonto_id
 where b.saldo_nok + nvl(b.ikkekap_kreditrente_belop, 0) + nvl(b.ikkekap_debetrente_belop, 0) < 0
   and k.tid_id = '&uttrekksdato'
   and k.batch_navn = '&batch_navn'
union
select tid.tid_id,
       tid.dato,
       k.sk_bankkunde_biii_id,
       k.rk_bankkonto_id,
       k.bk_sb1_selskap_id,
       k.kontonummer,
       least(b.saldo_nok + nvl(b.ikkekap_kreditrente_belop, 0) + nvl(b.ikkekap_debetrente_belop, 0), 0) as gjenvinning_belop
  from {{ ref('m_d_bankkunde_biii_lgd_t') }} k -- materialisert tabell iht. sb1_lgd.p_lgd_last
  left join kks kks on kks.bk_bankkonto_id = k.kontonummer
                   and kks.scd_gyldig_fom between k.sak_start_dato and k.beregn_til_dato
  join tid tid on tid.dato = kks.scd_gyldig_fom
  join {{ source('LGD_SOURCES', 'F_KONTOBALANSE_T') }} b on b.tid_id = tid_forrige_virkedag_id
                                 and b.rk_bankkonto_id = k.rk_bankkonto_id
 where b.saldo_nok + nvl(b.ikkekap_kreditrente_belop, 0) + nvl(b.ikkekap_debetrente_belop, 0) < 0
  and k.tid_id = '&uttrekksdato'
  and k.batch_navn = '&batch_navn'
