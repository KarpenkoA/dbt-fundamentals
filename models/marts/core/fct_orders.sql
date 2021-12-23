with payments as 
(
    select 
          order_id,
          sum(case when status = 'success' then amount end) as amount
        from {{ref("stg_payments")}}
     group by 1   
), final as (
select 
   a.order_id
 , b.customer_id
 , b.order_date
 , coalesce(a.amount,0) as amount
from payments a
    inner join {{ref("stg_orders")}} b
    on a.order_id = b.order_id
)

select * from final