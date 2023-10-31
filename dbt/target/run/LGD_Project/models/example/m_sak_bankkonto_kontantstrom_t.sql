
  create or replace   view RISIKO.LGD.m_sak_bankkonto_kontantstrom_t
  
   as (
    
      with
      diskonteringsrente as (
        select dato, rente_navn, rente_ppoeng, rente_referanse_ppoeng
        from (
                select t.dato,
                     trim(r.rentenavn) as rente_navn,
                     to_char(coalesce(r.rentesats, lag(r.rentesats) over (partition by trim(r.rentenavn) order by dato))) as rente_ppoeng,  /*Det finnes enkelte hull rente-tidsserien, aldri mer enn 1 dag*/
                     coalesce(r.rentesats, lag(r.rentesats) over (partition by trim(r.rentenavn) order by dato)) + 5 as rente_referanse_ppoeng,
                     rank()over(partition by dato order by gjeldende_fra_dato desc) as rnk
                from RISIKO.LGD.d_virkedag t
                left join RISIKO.LGD.D_RENTE_BASIS_SATS r on r.gjeldende_fra_dato between t.forrige_virkedag_dato and t.dato
                     and trim(r.rentenavn) = 'NIBOR3M'
                where r.rentenavn is not null
             ) where rnk = 1
      ),
      
      syndikat as (
        select s.tid_id, s.kontonummer_hovedandel
          from RISIKO.LGD.F_KNYTNING_SYNDIKAT s
        group by s.tid_id, s.kontonummer_hovedandel
        ),
      cashpool as (
        select cp.bk_bankkonto_id, cp.balansekonto_sb1_flagg, cp.scd_gyldig_fom, cp.scd_gyldig_tom
        from RISIKO.LGD.D_CASHPOOLHIERARKI cp 
        where cp.scd_slettet_i_kilde_dato is null
      ),
      kks as (
        select kks.bk_bankkonto_id, kks.scd_gyldig_fom, kks.scd_gyldig_tom
        from RISIKO.LGD.v_reskontro_kks_underkonto kks 
      ),

   konto_lgd as (
        select distinct ku.tid_id, ku.maletidspunkt_kode, flagg.kontantstrom_kilde_kode, ku.sk_bankkunde_biii_id, ku.rk_bankkunde_id, ku.rk_bankkonto_id, ku.bk_sb1_selskap_id, ku.kundenummer, ku.sak_start_dato, ku.tilfrisket_dato, ku.kontonummer, ku.konstatert_tap_dato,
               koflagg.eksponering_flagg, koflagg.kredittforetak_flagg, koflagg.syndikat_flagg, koflagg.eierbytte_flagg, koflagg.rk_trekkonto_utenfor_lgd_flagg, koflagg.korr_kilde_trans_flagg, koflagg.korr_kilde_gl_flagg, flagg.korr_kilde_konflikt_flagg,
               koflagg.ekskludert_konto_flagg, koflagg.ekskludert_konto_arsak, koflagg.annet_spesielt, ku.sak_start_tid_id, ku.beregn_til_dato, ku.beregn_til_tid_id,
               cp.balansekonto_sb1_flagg, s.kontonummer_hovedandel, kksx.bk_bankkonto_id as kks_bk_bankkonto_id
          from RISIKO.LGD.m_d_bankkunde_biii_lgd_t ku 
          join RISIKO.LGD.P_LGD_M_KONFIGURASJON kb on kb.maletidspunkt_kode = ku.maletidspunkt_kode
         
          join RISIKO.LGD.m_d_bankkunde_biii_flagg_t flagg on flagg.tid_id = '&uttrekksdato'
                                                       and flagg.batch_navn = '&batch_navn'
                                                       and flagg.sk_bankkunde_biii_id = ku.sk_bankkunde_biii_id
                                                       and flagg.maletidspunkt_kode = ku.maletidspunkt_kode
          join RISIKO.LGD.m_d_bankkonto_biii_flagg_t koflagg on koflagg.tid_id = '&uttrekksdato'
                                                         and koflagg.batch_navn = '&batch_navn'
                                                         and koflagg.sk_bankkunde_biii_id = ku.sk_bankkunde_biii_id
                                                         and koflagg.rk_bankkonto_id = ku.rk_bankkonto_id
                                                         and koflagg.maletidspunkt_kode = ku.maletidspunkt_kode
        left join cashpool cp on cp.bk_bankkonto_id = ku.kontonummer
                             and (ku.sak_start_dato between cp.scd_gyldig_fom and cp.scd_gyldig_tom
                                 or ku.beregn_til_dato between cp.scd_gyldig_fom and cp.scd_gyldig_tom)

        left join syndikat s on s.tid_id between ku.sak_start_tid_id and ku.beregn_til_tid_id
                            and s.kontonummer_hovedandel = ku.kontonummer
                            
        left join kks kksx on kksx.bk_bankkonto_id = ku.kontonummer
                          and (ku.sak_start_dato between kksx.scd_gyldig_fom and kksx.scd_gyldig_tom
                              or ku.beregn_til_dato between kksx.scd_gyldig_fom and kksx.scd_gyldig_tom)

          where ku.tid_id = '&uttrekksdato'
          and   ku.batch_navn = '&batch_navn'   

      )
select 
       ku.tid_id, ku.maletidspunkt_kode, ku.kontantstrom_kilde_kode, ku.sk_bankkunde_biii_id, ku.rk_bankkunde_id, ku.rk_bankkonto_id, ku.bk_sb1_selskap_id, ku.sak_start_dato, ku.tilfrisket_dato, ku.kontonummer, ku.eksponering_flagg, tid.tid_id kontantstrom_tid_id, tid.dato kontantstrom_dato,
       nvl(k.gjenvinning_belop, 0)
         + case when nvl(k2.gjenvinning_belop, 0) < 0 and nvl(k.gjenvinning_belop, 0) >= 0 then greatest(k2.gjenvinning_belop, -nvl(k.gjenvinning_belop, 0)) else 0 end
         + nvl(k3.gjenvinning_belop, 0)
         + nvl(tk1a.gjenvinning_belop, 0)
         + nvl(tk1b.gjenvinning_belop, 0)
         + nvl(tk2.gjenvinning_belop, 0)
         + nvl(tk3.gjenvinning_belop, 0) kontantstrom_belop,
       round(
         nvl(disc_naverdi(k.gjenvinning_belop, dr.rente_referanse_ppoeng, ku.sak_start_dato, k.dato), 0)
           + nvl(disc_naverdi(case when nvl(k2.gjenvinning_belop, 0) < 0 and nvl(k.gjenvinning_belop, 0) >= 0 then greatest(k2.gjenvinning_belop, -nvl(k.gjenvinning_belop, 0)) else 0 end, dr.rente_referanse_ppoeng, ku.sak_start_dato, k2.dato), 0)
           + nvl(disc_naverdi(k3.gjenvinning_belop, dr.rente_referanse_ppoeng, ku.sak_start_dato, k3.dato), 0)
           + nvl(disc_naverdi(tk1a.gjenvinning_belop, dr.rente_referanse_ppoeng, ku.sak_start_dato, tk1a.dato), 0)
           + nvl(disc_naverdi(tk1b.gjenvinning_belop, dr.rente_referanse_ppoeng, ku.sak_start_dato, tk1b.dato), 0)
           + nvl(disc_naverdi(tk2.gjenvinning_belop, dr.rente_referanse_ppoeng, ku.sak_start_dato, tk2.dato), 0)
           + nvl(disc_naverdi(tk3.gjenvinning_belop, dr.rente_referanse_ppoeng, ku.sak_start_dato, tk3.dato), 0)
         , 2) naverdi_kontantstrom_belop,
       case when (ku.eksponering_flagg = '1'
                  or ku.balansekonto_sb1_flagg = '1'
                  or ku.kontonummer_hovedandel is not null)
                 and not nvl(ku.balansekonto_sb1_flagg, '1') = '0'
                 and ku.kks_bk_bankkonto_id is null
                  /* Unnlater å regne med gjenvinninger på dato for konstatert tap, da dette som regel er feilkodet tapsføring */
                 and not (nvl(ku.konstatert_tap_dato, tid.dato - 1) = tid.dato   /* Manglende konstatert tap-dato skal ikke gi match */
                          and (nvl(k.gjenvinning_belop, 0)
                               + case when nvl(k2.gjenvinning_belop, 0) < 0 and nvl(k.gjenvinning_belop, 0) >= 0 then greatest(k2.gjenvinning_belop, -nvl(k.gjenvinning_belop, 0)) else 0 end
                               + nvl(k3.gjenvinning_belop, 0)
                               + nvl(tk1a.gjenvinning_belop, 0)) > 0)
              then '1'
              else '0'
        end gyldig_kontantstrom_flagg,
       k.gjenvinning_belop gl_belop,
       case when nvl(k2.gjenvinning_belop, 0) < 0 and nvl(k.gjenvinning_belop, 0) >= 0 then greatest(k2.gjenvinning_belop, -nvl(k.gjenvinning_belop, 0)) else 0 end gl_korr_henl_saldo_belop,
       k2.gjenvinning_belop ujust_korr_henl_saldo_belop, k3.gjenvinning_belop gl_korr_over_underkurs_belop, tk1a.gjenvinning_belop kaptrans_belop, tk1b.gjenvinning_belop syndikat_deltaker_belop, tk2.gjenvinning_belop rentetrekk_belop, tk3.gjenvinning_belop cp_kks_exit_belop,
       round(disc_naverdi(k.gjenvinning_belop, dr.rente_referanse_ppoeng, ku.sak_start_dato, k.dato), 2) nv_gl_belop,
       round(disc_naverdi(case when nvl(k2.gjenvinning_belop, 0) < 0 and nvl(k.gjenvinning_belop, 0) >= 0 then greatest(k2.gjenvinning_belop, -nvl(k.gjenvinning_belop, 0)) else 0 end, dr.rente_referanse_ppoeng, ku.sak_start_dato, k2.dato), 2) nv_gl_korr_henl_saldo_belop,
       round(disc_naverdi(k3.gjenvinning_belop, dr.rente_referanse_ppoeng, ku.sak_start_dato, k3.dato), 2) nv_gl_korr_o_underkurs_belop,
       round(disc_naverdi(tk1a.gjenvinning_belop, dr.rente_referanse_ppoeng, ku.sak_start_dato, tk1a.dato), 2) nv_kaptrans_belop,
       round(disc_naverdi(tk1b.gjenvinning_belop, dr.rente_referanse_ppoeng, ku.sak_start_dato, tk1b.dato), 2) nv_syndikat_deltakr_belop,
       round(disc_naverdi(tk2.gjenvinning_belop, dr.rente_referanse_ppoeng, ku.sak_start_dato, tk2.dato), 2) nv_rentetrekk_belop,
       round(disc_naverdi(tk3.gjenvinning_belop, dr.rente_referanse_ppoeng, ku.sak_start_dato, tk3.dato), 2) nv_cp_kks_exit_belop,
       ku.kredittforetak_flagg, ku.syndikat_flagg, ku.eierbytte_flagg, ku.rk_trekkonto_utenfor_lgd_flagg, ku.korr_kilde_trans_flagg, ku.korr_kilde_gl_flagg, ku.korr_kilde_konflikt_flagg, ku.ekskludert_konto_flagg, ku.ekskludert_konto_arsak, ku.annet_spesielt,
       dr.rente_navn, dr.rente_ppoeng, dr.rente_referanse_ppoeng,
       '&batch_navn' as batch_navn
       
        from konto_lgd ku
        join RISIKO.LGD.d_virkedag tid on tid.dato between ku.sak_start_dato and ku.beregn_til_dato 
        /* Holder kontantstrømmer for saker med sak_start_dato < 1.1.2009 utenfor, saker er da ev. med i datasettet av andre årsaker enn å beregne historisk LGD*/
        left join RISIKO.LGD.m_gl_kontantstrom_t k on k.dato = tid.dato
                                             and k.sk_bankkunde_biii_id = ku.sk_bankkunde_biii_id
                                             and k.kontonummer = ku.kontonummer
                                             and k.trans_kilde_kode = 'gl'
                                             and ku.kontantstrom_kilde_kode = 'GL'
                                             and ku.sak_start_dato >= to_date('20090101', 'yyyymmdd')
                                             and ku.maletidspunkt_kode = k.maletidspunkt_kode
                                             and k.tid_id = '&uttrekksdato'
                                             and k.batch_navn = '&batch_navn'
        left join RISIKO.LGD.m_gl_kontantstrom_t k2 on k2.dato = tid.dato
                                              and k2.sk_bankkunde_biii_id = ku.sk_bankkunde_biii_id
                                              and k2.kontonummer = ku.kontonummer
                                              and k2.trans_kilde_kode = 'HENLEGGELSE_SALDO'
                                              and ku.kontantstrom_kilde_kode = 'GL'
                                              and ku.sak_start_dato >= to_date('20090101', 'yyyymmdd')
                                              and ku.maletidspunkt_kode = k2.maletidspunkt_kode
                                              and k2.tid_id = '&uttrekksdato'
                                              and k2.batch_navn = '&batch_navn'

        left join RISIKO.LGD.m_gl_kontantstrom_t k3 on k3.dato = tid.dato
                                              and k3.sk_bankkunde_biii_id = ku.sk_bankkunde_biii_id
                                              and k3.kontonummer = ku.kontonummer
                                              and k3.trans_kilde_kode = 'OVER-UNDERKURS'
                                              and ku.kontantstrom_kilde_kode = 'GL'
                                              and ku.sak_start_dato >= to_date('20090101', 'yyyymmdd')
                                              and ku.maletidspunkt_kode = k3.maletidspunkt_kode
                                              and k3.tid_id = '&uttrekksdato'
                                              and k3.batch_navn = '&batch_navn'

        left join RISIKO.LGD.m_trans_kontantstrom_t  tk1a on tk1a.dato = tid.dato
                                                   and tk1a.sk_bankkunde_biii_id = ku.sk_bankkunde_biii_id
                                                   and tk1a.rk_bankkonto_id = ku.rk_bankkonto_id
                                                   and tk1a.trans_kilde_kode = 'ordinær'
                                                   and ku.kontantstrom_kilde_kode = 'TRANS'
                                                   and ku.sak_start_dato >= to_date('20090101', 'yyyymmdd')
                                                   and ku.maletidspunkt_kode = tk1a.maletidspunkt_kode
                                                   and tk1a.tid_id = '&uttrekksdato'
                                                   and tk1a.batch_navn = '&batch_navn'
                                                   
        left join RISIKO.LGD.m_trans_kontantstrom_t tk1b on tk1b.dato = tid.dato
                                                   and tk1b.sk_bankkunde_biii_id = ku.sk_bankkunde_biii_id
                                                   and tk1b.rk_bankkonto_id = ku.rk_bankkonto_id
                                                   and tk1b.trans_kilde_kode = 'syndikat'
                                                   and ku.kontantstrom_kilde_kode = 'TRANS'
                                                   and ku.sak_start_dato >= to_date('20090101', 'yyyymmdd')
                                                   and ku.maletidspunkt_kode = tk1b.maletidspunkt_kode
                                                   and tk1b.tid_id = '&uttrekksdato'
                                                   and tk1b.batch_navn = '&batch_navn'
                                                   
        left join RISIKO.LGD.m_trans_kontantstrom_rente_t tk2 on tk2.dato = tid.dato
                                                        and tk2.sk_bankkunde_biii_id = ku.sk_bankkunde_biii_id
                                                        and tk2.rk_bankkonto_id = ku.rk_bankkonto_id
                                                        and ku.kontantstrom_kilde_kode = 'TRANS'
                                                        and ku.sak_start_dato >= to_date('20090101', 'yyyymmdd')
                                                        and ku.maletidspunkt_kode = tk2.maletidspunkt_kode
                                                        and tk2.tid_id = '&uttrekksdato'
                                                        and tk2.batch_navn = '&batch_navn'

        left join RISIKO.LGD.v_trans_kontantstrom_cp_kks tk3 on tk3.dato = tid.dato
                                                         and tk3.sk_bankkunde_biii_id = ku.sk_bankkunde_biii_id
                                                         and tk3.rk_bankkonto_id = ku.rk_bankkonto_id
                                                         and ku.kontantstrom_kilde_kode = 'TRANS'
                                                         and ku.sak_start_dato >= to_date('20090101', 'yyyymmdd')

        left join diskonteringsrente dr on dr.dato = ku.sak_start_dato

       where /* ønsker i hovedsak reelle kontantstrømmer*/
       ((ku.eksponering_flagg = '1'
         or ku.balansekonto_sb1_flagg = '1'
         or ku.kontonummer_hovedandel is not null)
        and not nvl(ku.balansekonto_sb1_flagg, '1') = '0'
        and ku.kks_bk_bankkonto_id is null
        and nvl(k.gjenvinning_belop, 0)
             + nvl(k2.gjenvinning_belop, 0)
             + nvl(k3.gjenvinning_belop, 0)
             + nvl(tk1a.gjenvinning_belop, 0)
             + nvl(tk1b.gjenvinning_belop, 0)
             + nvl(tk2.gjenvinning_belop, 0)
             + nvl(tk3.gjenvinning_belop, 0) <> 0)
    or tid.dato = ku.sak_start_dato /* ønsker minimum 1 rad per bankkonto ut av viewet */
  );

