
  create or replace   view RISIKO.LGD.v_reskontro_kks_underkonto
  
   as (
    
select
/**********************************************************************************************
Beskrivelse: Identifiserer SCD-rader der en konto er en KKS-underkonto
             (og ikke balanseført utenom på hovedkontoen).


Tabellgrunnlag: d_reskontro


Endringslogg:
Initialier   Dato         Beskrivelse
MBJ          16.12.20     Opprettet view

***********************************************************************************************/
 rk.rk_bankkonto_id,
 rk.bk_bankkonto_id,
 rk.kks_kode,
 rk.kks_hovedkonto_nummer,
 rk.kks_eierkonto_nummer,
 rk.kks_konto_rolle_kode,
 rk.kks_konto_type_kode,
 rk.scd_gyldig_fom,
 rk.scd_gyldig_tom,
 rk.scd_aktiv_flagg
  from RISIKO.LGD.D_RESKONTRO rk
 where rk.kks_hovedkonto_nummer is not null
   and rk.kks_hovedkonto_nummer <> rk.bk_bankkonto_id
   and rk.kks_konto_type_kode = 'KOVF'
  );

