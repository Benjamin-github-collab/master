
    
    

select
    sk_bankkunde_biii_id as unique_field,
    count(*) as n_records

from RISIKO.LGD.v_d_bankkunde_biii_korr
where sk_bankkunde_biii_id is not null
group by sk_bankkunde_biii_id
having count(*) > 1


