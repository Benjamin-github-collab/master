
  
    

        create or replace transient table RISIKO.LGD.m_gl_kontantstrom_temp1
         as
        (

select 
dato,
tid_id,
forrige_virkedag_tid_id,
bk_sb1_selskap_id,
sk_bankkunde_biii_id,
rk_bankkonto_id,
kontonummer,
kontantstrom,
p_belop,
ul_flagg,
trans_kilde_kode,
konto_sak_konstatert_tap_dato
from (

with
tid as (
  select /*+ mateialize */ t.tid_id, t.dato, v.tid_id as virkedag_tid_id, v.dato as virkedag_dato, v.forrige_virkedag_tid_id, v.forrige_virkedag_dato, v.neste_virkedag_tid_id, v.neste_virkedag_dato
    from RISIKO.LGD.D_TID t
    join RISIKO.LGD.d_virkedag v on t.tid_id < v.neste_virkedag_tid_id and t.tid_id >= v.tid_id
),
st_trans as (
select /*+ parallel(t,4) full(k) full(kb)*/
       'gl' as trans_kilde_kode,
       tid.virkedag_dato as dato,
       tid.virkedag_tid_id as tid_id,
       tid.forrige_virkedag_tid_id,       
       k.sk_bankkunde_biii_id,
       k.rk_bankkonto_id,
       t.kontonummer,
       t.gl_konto_id art,
       t.klassifisering_type,
       t.transaksjon_belop belop,
       t.gl_system_id systemkode,
       k.bk_sb1_selskap_id,
       k.konstatert_tap_dato konto_sak_konstatert_tap_dato
  from RISIKO.LGD.m_d_bankkunde_biii_lgd_t k 
  join RISIKO.LGD.M_KONFIGURASJON_BANK kb on kb.bk_sb1_selskap_id = k.bk_sb1_selskap_id
                                      and kb.maletidspunkt_kode = k.maletidspunkt_kode

  join RISIKO.LGD.F_GL_BANKKONTO_TRANS_B t on t.kontonummer = k.kontonummer
                                       /* må få med transaksjoner i sak, ink. som kan bli flyttet tilbake til en virkedag, tidskriterie er likevel med her, og ikke bare i where, for bedre ytelse*/
                                       and t.transaksjon_dato between k.sak_start_dato and k.beregn_til_dato + 7
  join tid tid on t.transaksjon_dato = tid.dato
 where tid.virkedag_dato <= k.beregn_til_dato
 and k.tid_id = 'uttrekksdato'
 and k.batch_navn = 'batch_navn'

union all   
select /*+ parallel(t,4) full(k) full(kb) full(t) full(ko)*/
       ko.kategori trans_kilde_kode,
       tid.dato,
       t.tid_id as tid_id,
       tid.forrige_virkedag_tid_id,
       k.sk_bankkunde_biii_id,
       k.rk_bankkonto_id,
       k.kontonummer,
       null art,
       null klassifisering_type,
       case ko.snu_gl_fortegn when '1' then -1 else 1 end * t.transaksjonsbelop_nok belop,
       'KAPTRANS' systemkode,
       k.bk_sb1_selskap_id,
       k.konstatert_tap_dato konto_sak_konstatert_tap_dato
 from RISIKO.LGD.m_d_bankkunde_biii_lgd_t k
 join RISIKO.LGD.M_KONFIGURASJON_BANK kb on kb.bk_sb1_selskap_id = k.bk_sb1_selskap_id
                                     and kb.maletidspunkt_kode = k.maletidspunkt_kode

 join RISIKO.LGD.F_KAPITALTRANSAKSJON_T t on t.tid_id between k.sak_start_tid_id and k.beregn_til_tid_id
                                        and t.rk_bankkonto_id = k.rk_bankkonto_id
 join RISIKO.LGD.P_KAPITALTRANSAKSJONSKODE ko on t.bk_transaksjonskode_id = ko.kode
                                          and ko.korriger_gl_flagg = '1'
 join tid tid on tid.tid_id = t.tid_id
 where k.tid_id = '&uttrekksdato'
 and k.batch_navn = '&batch_navn'
 )
  select t.dato,
         t.tid_id,
         t.forrige_virkedag_tid_id,
         t.bk_sb1_selskap_id,
         t.sk_bankkunde_biii_id,
         t.rk_bankkonto_id,
         t.kontonummer,
         sum(-t.belop) kontantstrom,
         sum(case when t.klassifisering_type = 'P' then t.belop else 0 end) p_belop,
         max(case when t.systemkode = 'UL' then '1' else '0' end) as ul_flagg,
         t.trans_kilde_kode,
         t.konto_sak_konstatert_tap_dato
    from st_trans t
   where t.systemkode in ('RK', 'UL', 'VR', 'GA', 'SC', 'KAPTRANS')
     and (t.klassifisering_type in ('P', 'A') or t.systemkode = 'KAPTRANS')
   group by t.dato,
            t.tid_id,
            t.forrige_virkedag_tid_id,
            t.bk_sb1_selskap_id,
            t.sk_bankkunde_biii_id,
            t.rk_bankkonto_id,
            t.kontonummer,
            t.trans_kilde_kode,
            t.konto_sak_konstatert_tap_dato
)
        );
      
  