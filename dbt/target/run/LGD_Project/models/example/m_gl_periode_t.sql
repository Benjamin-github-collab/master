
  
    

        create or replace transient table RISIKO.LGD.m_gl_periode_t
         as
        (
select
 '20230331' as tid_id,
 konf.maletidspunkt_kode,
 t.bankkode,
 t.gl_bankkode,
 case t.bankkode
   when '4702' then greatest('20160701', to_char(trunc(min(t.transaksjon_dato), 'MM'), 'YYYYMMDD'))
   else to_char(trunc(min(t.transaksjon_dato), 'MM'), 'YYYYMMDD')
  end trans_fom_tid_id,
 to_char(last_day(max(t.transaksjon_dato)), 'YYYYMMDD') trans_tom_tid_id,
 case t.bankkode
   when '4702' then greatest(to_date('20160701', 'yyyymmdd'), trunc(min(t.transaksjon_dato), 'MM'))
   else trunc(min(t.transaksjon_dato), 'MM')
  end trans_fom_dato, 
 last_day(max(t.transaksjon_dato)) trans_tom_dato,
 max(date(CURRENT_TIMESTAMP)) CURRENT_TIMESTAMP,
 'batch_navn' as batch_navn
  from RISIKO.LGD.F_GL_BANKKONTO_TRANS_B t
  cross join RISIKO.LGD.P_LGD_M_KONFIGURASJON konf
 group by t.bankkode, t.gl_bankkode,konf.maletidspunkt_kode
        );
      
  