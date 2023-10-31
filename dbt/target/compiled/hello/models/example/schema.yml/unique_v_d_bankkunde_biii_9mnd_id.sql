
    
    

select
    id as unique_field,
    count(*) as n_records

from RISIKO.LGD.v_d_bankkunde_biii_9mnd
where id is not null
group by id
having count(*) > 1


