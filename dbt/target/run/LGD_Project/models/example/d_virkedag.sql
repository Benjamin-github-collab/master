
  
    

        create or replace transient table RISIKO.LGD.d_virkedag
         as
        (
SELECT
     tid_id as tid_id,
     dato as dato,
     lag(tid_id) over (order by tid_id) forrige_virkedag_tid_id,
     lag(dato) over (order by tid_id) forrige_virkedag_dato,
     lead(tid_id) over (order by tid_id) neste_virkedag_tid_id,
     lead(dato) over (order by tid_id) neste_virkedag_dato,
     tid_id_depot siste_manedslast_tid_id
FROM RISIKO.LGD.D_TID 
WHERE virkedag_flagg = '1'
    AND tid_id BETWEEN '20040101' AND TO_CHAR(DATEADD(MONTH, 12, CURRENT_DATE), 'yyyymmdd')
        );
      
  