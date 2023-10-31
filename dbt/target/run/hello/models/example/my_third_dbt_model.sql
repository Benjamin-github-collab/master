
  create or replace   view RISIKO.LGD.my_third_dbt_model
  
   as (
    

select *
from RISIKO.LGD.my_first_dbt_model
--where id = 1
  );

