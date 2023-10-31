
  create or replace   view RISIKO.LGD.v_cashpool_ikke_balanse
  
   as (
    select
/**********************************************************************************************
Beskrivelse: Identifiserer SCD-rader der en konto er en ikke-balanseført Cash Pool-konto
             (kun balanseført på GCA-kontoen i samme valuta i hierarkiet).


Tabellgrunnlag: d_cashpoolhierarki


Endringslogg:
Initialier   Dato         Beskrivelse
MBJ          16.12.20     Opprettet view

***********************************************************************************************/
 cp.rk_bankkonto_id,
 cp.bk_bankkonto_id,
 cp.bk_sb1_selskap_id,
 cp.cashpoolkonto_type,
 cp.cashpoolkonto_status,
 cp.balansekonto_sb1_flagg,
 cp.scd_slettet_i_kilde_dato,
 cp.scd_gyldig_fom,
 cp.scd_gyldig_tom
  from RISIKO.LGD.D_CASHPOOLHIERARKI cp
 where cp.balansekonto_sb1_flagg = '0'
   and cp.scd_slettet_i_kilde_dato is null
  );

