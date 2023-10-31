
with
trans as (
select t.tid_id,
       tid.dato,
       k.sk_bankkunde_biii_id,
       t.kontonummer,
       t.transaksjonsbelop_nok,
       t.transaksjonsbelop_valuta,
       t.bk_avleverende_system_id,
       t.bk_transaksjonskode_id,
       'ordinær' trans_kilde_kode,
       tid.forrige_virkedag_tid_id as tid_forrige_virkedag_id,
       t.bk_sb1_selskap_id,
       t.rk_bankkonto_id
  from RISIKO.LGD.m_d_bankkunde_biii_lgd_t k 
  join RISIKO.LGD.M_KONFIGURASJON_BANK kb on kb.maletidspunkt_kode = k.maletidspunkt_kode
                                      and kb.bk_sb1_selskap_id = k.bk_sb1_selskap_id
  join RISIKO.LGD.F_KAPITALTRANSAKSJON_T t on t.rk_bankkonto_id = k.rk_bankkonto_id
                                         and t.tid_id between k.sak_start_tid_id and k.beregn_til_tid_id
  join RISIKO.LGD.d_virkedag tid on t.tid_id = tid.tid_id
  left join RISIKO.LGD.D_CASHPOOLHIERARKI cash on cash.bk_bankkonto_id = t.kontonummer
                                                  and tid.dato between cash.scd_gyldig_fom and cash.scd_gyldig_tom
                                                  and cash.scd_slettet_i_kilde_dato is null
  left join RISIKO.LGD.v_reskontro_kks_underkonto kks on kks.bk_bankkonto_id = t.kontonummer
                                                  and tid.dato between kks.scd_gyldig_fom and kks.scd_gyldig_tom
 where nvl(cash.balansekonto_sb1_flagg, '1') = '1'
   and kks.bk_bankkonto_id is null
   and k.tid_id = '&uttrekksdato'
   and k.batch_navn = '&batch_navn'
),
trans_fra_synd_del as (
select t.tid_id,
       tid.dato,
       k.sk_bankkunde_biii_id,
       s.kontonummer_hovedandel as kontonummer,
       t.transaksjonsbelop_nok,
       t.transaksjonsbelop_valuta,
       t.bk_avleverende_system_id,
       t.bk_transaksjonskode_id,
       'syndikat' trans_kilde_kode,
       tid.forrige_virkedag_tid_id as tid_forrige_virkedag_id,
       t.bk_sb1_selskap_id,
       s.rk_bankkonto_hovedandel_id as rk_bankkonto_id
  from RISIKO.LGD.m_d_bankkunde_biii_lgd_t k /* materialisert tabell iht. sb1_lgd.p_lgd_last*/
  join RISIKO.LGD.M_KONFIGURASJON_BANK kb on kb.maletidspunkt_kode = k.maletidspunkt_kode
                                      and kb.bk_sb1_selskap_id = k.bk_sb1_selskap_id
  join RISIKO.LGD.F_KNYTNING_SYNDIKAT s on k.kontonummer = s.kontonummer_hovedandel

  join RISIKO.LGD.m_kapitaltransaksjon_lgd t on t.rk_bankkonto_id = s.rk_bankkonto_deltakerandel_id
                                         and t.tid_id = s.tid_id
                                         and t.tid_id between k.sak_start_tid_id and k.beregn_til_tid_id

  join RISIKO.LGD.d_virkedag tid on t.tid_id = tid.tid_id
  where k.tid_id = '&uttrekksdato'
   and k.batch_navn = '&batch_navn'
),
m_trans_kontantstrom_temp as  (select * from (select t.tid_id,
               t.dato,
               t.sk_bankkunde_biii_id,
               t.kontonummer,
               sum(case when not (f.kode is not null
                                  and nvl(t.bk_avleverende_system_id, 'x') not in ('30', '31'))
                          then t.transaksjonsbelop_nok
                        else 0
                    end) kontantstrom_belop,
               sum(case when f.kode is not null
                             and nvl(t.bk_avleverende_system_id, 'x') not in ('30', '31')
                          then t.transaksjonsbelop_nok
                        else 0 end) rente_gebyr_belop,
               max(case when substr(t.bk_transaksjonskode_id, 1, 1) = 'U' then '1' else '0' end) u_flagg,
               t.trans_kilde_kode,
               t.tid_forrige_virkedag_id,
               t.bk_sb1_selskap_id,
               t.rk_bankkonto_id,
               sum(t.transaksjonsbelop_nok) kontantstrom_brutto,
               sum(t.transaksjonsbelop_valuta) kontantstrom_valuta_brutto
          from (select * from trans
                union all
                select * from trans_fra_synd_del
               ) t
               left join RISIKO.LGD.P_KAPITALTRANSAKSJONSKODE f on f.kode = t.bk_transaksjonskode_id
         where nvl(f.kategori, 'x') <> 'HENLEGGELSE_SALDO'
         group by t.tid_id, t.dato, t.tid_forrige_virkedag_id, t.sk_bankkunde_biii_id, t.rk_bankkonto_id, t.bk_sb1_selskap_id, t.kontonummer, t.trans_kilde_kode
        )
 where kontantstrom_belop <> 0
    or rente_gebyr_belop <> 0
)



select '&uttrekksdato' as tid_id,
       g.maletidspunkt_kode,
       g.dato,
       g.sk_bankkunde_biii_id,
       g.kontonummer,
       case g.u_flagg
         when '1' then g.kontantstrom_belop
         else g.rente_dekket_av_innskudd_belop + g.kontantstrom_ut_belop + g.kontantstrom_inn_belop
        end as gjenvinning_belop,
       g.rente_dekket_av_innskudd_belop, /* Ev. positiv saldo forrige dag vil kunne gi rentegjenvinning ved rentebelastning*/
       g.kontantstrom_ut_belop,
       g.kontantstrom_inn_belop,
       g.u_flagg,
       g.saldo_forrige_dag,
       g.saldo_samme_dag,
       g.trans_kilde_kode,
       g.avvik_kontobalanse_trans_flagg,
       g.bk_sb1_selskap_id,
       g.rk_bankkonto_id,
       g.kontantstrom_belop,
       g.rente_gebyr_belop,
       g.kontantstrom_brutto,
       g.kontantstrom_valuta_brutto,
       '&batch_navn' as batch_navn
from (
select 
               t.tid_id,
               t.dato,
               konf.maletidspunkt_kode,
               t.sk_bankkunde_biii_id,
               t.kontonummer,
               least(greatest(nvl(bal1.saldo_nok, 0), 0), -least(t.rente_gebyr_belop, 0)) as rente_dekket_av_innskudd_belop, /* Ev. positiv saldo forrige dag vil kunne gi rentegjenvinning ved rentebelastning*/
               case when t.kontantstrom_belop < 0 and bal.saldo_nok < 0 /* Teller kun kontantstrøm ut dersom det faktisk er en faktisk overføring ut, samt at saldo ender i minus*/
                      then t.kontantstrom_belop + case when t.rente_gebyr_belop < 0 then greatest(greatest(nvl(bal1.saldo_nok, 0), 0) + t.rente_gebyr_belop, 0) /* Ved rentebelasning, korrigerer for ev. innskudd dagen før etter at renter er betalt*/
                                                       else greatest(nvl(bal1.saldo_nok, 0) + t.rente_gebyr_belop, 0) end /* Ved rentereversering/innskuddsrente, korrigerer for ev. positiv inngående saldo etter renteinngangen*/
                 else 0 end as kontantstrom_ut_belop,
               case when t.kontantstrom_belop > 0 and greatest(nvl(bal1.saldo_nok, 0), 0) + least(t.rente_gebyr_belop, 0) <= 0 /* Teller kun kontanstrøm inn dersom det faktisk er en overføring inn, og at saldo forrige dag + ev. rentereversering/inngang ikke er positiv*/
                      then greatest(t.kontantstrom_belop - greatest(bal.saldo_nok, 0), 0) /* Korrigerer for positiv saldo etter inngang, men unngår overkompensering og negativt tall*/
                    else 0 end as kontantstrom_inn_belop,
               t.u_flagg,
               bal1.saldo_nok saldo_forrige_dag,
               bal.saldo_nok saldo_samme_dag,
               t.trans_kilde_kode,
               case when t.kontantstrom_valuta_brutto <> nvl(bal.saldo_valuta, 0) - nvl(bal1.saldo_valuta, 0) then '1' else '0' end avvik_kontobalanse_trans_flagg,
               t.bk_sb1_selskap_id,
               t.rk_bankkonto_id,
               t.kontantstrom_belop,
               t.rente_gebyr_belop,
               t.kontantstrom_brutto,
               t.kontantstrom_valuta_brutto
          from m_trans_kontantstrom_temp t
          left join RISIKO.LGD.F_KONTOBALANSE_T bal on bal.tid_id = t.tid_id
                                                and bal.rk_bankkonto_id = t.rk_bankkonto_id
          left join RISIKO.LGD.F_KONTOBALANSE_T bal1 on bal1.tid_id = t.tid_forrige_virkedag_id
                                                 and bal1.rk_bankkonto_id = t.rk_bankkonto_id
          cross join RISIKO.LGD.P_LGD_M_KONFIGURASJON konf
         where t.u_flagg = '1'
            or least(greatest(nvl(bal1.saldo_nok, 0), 0), -least(t.rente_gebyr_belop, 0)) <> 0 /* rente_gebyr_belop*/
            or case when t.kontantstrom_belop < 0 and bal.saldo_nok < 0
                      then t.kontantstrom_belop + case when t.rente_gebyr_belop < 0 then greatest(greatest(nvl(bal1.saldo_nok, 0), 0) + t.rente_gebyr_belop, 0)
                                                       else greatest(nvl(bal1.saldo_nok, 0) + t.rente_gebyr_belop, 0) end
                    else 0 end <> 0 /* kontantstrom_ut_belop*/
            or case when t.kontantstrom_belop > 0 and greatest(nvl(bal1.saldo_nok, 0), 0) + least(t.rente_gebyr_belop, 0) <= 0 /* Teller kun kontanstrøm inn dersom det faktisk er en overføring inn, og at saldo forrige dag + ev. rentereversering/inngang ikke er positiv*/
                      then greatest(t.kontantstrom_belop - greatest(bal.saldo_nok, 0), 0) /* Korrigerer for positiv saldo etter inngang, men unngår overkompensering og negativt tall*/
                    else 0 end <> 0 /* kontantstrom_inn_belop*/
            ) g
 where case g.u_flagg when '1' then g.kontantstrom_belop else g.rente_dekket_av_innskudd_belop + g.kontantstrom_ut_belop + g.kontantstrom_inn_belop end <> 0 /* beregnet kontantstrøm <> 0 */