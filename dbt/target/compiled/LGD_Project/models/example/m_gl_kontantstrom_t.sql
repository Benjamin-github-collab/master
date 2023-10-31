
with nedskrivning as (select /*+ materialize*/ n.bk_bankkonto_id,
                             n.bk_sb1_selskap_forvalter_id,
                             n.nedskrivning_belop - nvl(lag(n.nedskrivning_belop) over(partition by n.bk_nedskrivning_id,
                                                                     n.bk_sb1_selskap_forvalter_id order by n.scf_gyldig_fom),
                                                                0) endring_nedskrivning_belop,
                             n.scf_gyldig_fom,
                             n.scf_gyldig_tom
                                from RISIKO.LGD.F_NEDSKRIVNING n), 
cashpool as (select cp.bk_bankkonto_id, cp.scd_gyldig_fom, cp.scd_gyldig_tom
             from RISIKO.LGD.v_cashpool_ikke_balanse cp
            ),
kks as (select kks_u.bk_bankkonto_id, kks_u.scd_gyldig_fom, kks_u.scd_gyldig_tom
        from RISIKO.LGD.v_reskontro_kks_underkonto kks_u
      ), 

m_gl_kontantstrom_temp2 as (
select 
   t.dato,
   t.tid_id,
   t.bk_sb1_selskap_id,
   t.sk_bankkunde_biii_id,
   t.rk_bankkonto_id,
   t.kontonummer,
   t.kontantstrom,
   t.p_belop,
   t.ul_flagg,
   t.forrige_virkedag_tid_id,
   n.endring_nedskrivning_belop,
   t.p_belop - nvl(n.endring_nedskrivning_belop, 0) as p_belop_korr, /* inntekt er - p? P, f.eks. -5000 betyr nedskrivning redusert med 5000, som vil si -5000 i nedskrivning-endring.*/
   t.trans_kilde_kode,
   t.konto_sak_konstatert_tap_dato
    from RISIKO.LGD.m_gl_kontantstrom_temp1 t
    left join cashpool cp on cp.bk_bankkonto_id = t.kontonummer
                         and t.dato between cp.scd_gyldig_fom and cp.scd_gyldig_tom
    left join kks kks on kks.bk_bankkonto_id = t.kontonummer
                     and t.dato between kks.scd_gyldig_fom and kks.scd_gyldig_tom
    left join nedskrivning n on n.bk_sb1_selskap_forvalter_id = t.bk_sb1_selskap_id
                            and n.bk_bankkonto_id = t.kontonummer
                            and n.scf_gyldig_fom = t.dato
                            and t.trans_kilde_kode = 'gl'
   where cp.bk_bankkonto_id is null
     and kks.bk_bankkonto_id is null

)                 
select '&uttrekksdato' as tid_id,
                konf.maletidspunkt_kode,
                t.dato,
                t.bk_sb1_selskap_id,
                t.sk_bankkunde_biii_id,
                t.rk_bankkonto_id,
                t.kontonummer,
                case
                  when ul_flagg = '1' or t.trans_kilde_kode <> 'gl' or t.konto_sak_konstatert_tap_dato < t.dato then
                   t.kontantstrom /* Ser bort fra fortegnet p? saldo for utl?nskontoer, korrigeringer og inngang konstaterte tap*/
                  else
                   case
                     when bal.saldo_nok >= 0 then
                      case
                        when nvl(bal1.saldo_nok, 0) >= 0 then
                         0
                        else
                         -nvl(bal1.saldo_nok, 0) - least(t.p_belop_korr, -nvl(bal1.saldo_nok, 0))
                      end
                     else /*saldo_samme_dag < 0*/
                      case
                        when nvl(bal1.saldo_nok, 0) >= 0 then
                         bal.saldo_nok - case
                           when t.p_belop_korr < 0 then
                            t.p_belop_korr + least(nvl(bal1.saldo_nok, 0), -least(t.p_belop_korr, 0))
                           else
                            0
                         end /* Trekker fra ikke-gjenvunnet renter/gebyrer, skal ikke telles som opptrekk*/
                         +least(nvl(bal1.saldo_nok, 0), -least(t.p_belop_korr, 0)) /* Legger til renter/gebyrer dekket av positiv saldo forrige dag (som opptrekk)*/
                        else
                         kontantstrom
                      end
                   end
                end as gjenvinning_belop,
                t.kontantstrom,
                t.p_belop,
                t.ul_flagg,
                t.endring_nedskrivning_belop,
                t.p_belop_korr,
                bal.saldo_nok saldo_samme_dag,
                nvl(bal1.saldo_nok, 0) saldo_forrige_dag,
                t.trans_kilde_kode,
                t.konto_sak_konstatert_tap_dato,
                '&batch_navn' as batch_navn
           from m_gl_kontantstrom_temp2 t
           join RISIKO.LGD.F_KONTOBALANSE_T bal on bal.tid_id = t.tid_id
                                            and bal.rk_bankkonto_id = t.rk_bankkonto_id
           left join RISIKO.LGD.F_KONTOBALANSE_T bal1 on bal1.tid_id = t.forrige_virkedag_tid_id
                                                  and bal1.rk_bankkonto_id = t.rk_bankkonto_id
           cross join RISIKO.LGD.P_LGD_M_KONFIGURASJON konf