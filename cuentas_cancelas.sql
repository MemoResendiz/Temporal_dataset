select anio_cancel,count(*) from(
SELECT 
account_no,
status,
created_t,
vida_util,
anio_created,
mes_created,
status_cuenta,
cancel_t,
case
       when status='10100' or status='10102' then 0
       else (CAST(SUBSTRING(cancel_t,1,4) as int))
       end as anio_cancel, case
       when status='10100' or status='10102' then 0
       else (CAST(SUBSTRING(cancel_t,6,2) as int))       
       end as mes_cancel FROM(
SELECT account_no, status,created_t,vida_util, (CAST(SUBSTRING(created_t,1,4) as int)) as anio_created, (CAST(SUBSTRING(created_t,6,2) as int)) as mes_created, status_cuenta,
 (DATEADD(month, vida_util ,date(created_t))) AS cancel_t FROM(
  select 
     account_no,
     status,created_t,
     case when status = '10103' then DATEDIFF(MM , date(created_t), date(from_unixtime(last_status_t)))
           when status='10102'  then DATEDIFF(MM , date(created_t), CURRENT_DATE ) 
           when status='10100' then DATEDIFF(MM , date(created_t), CURRENT_DATE ) 
      end as vida_util,
    
      case when status='10100'
        then 'Activo'
        When status='10102'
        then 'Inactivo'
        when status='10103'
        then 'Cancelada'
  end as status_cuenta
  from data_staging.brm_account_t where info_day= 20221226)
  
WHERE SUBSTRING(account_no,1,2) = '1.' OR SUBSTRING(account_no,1,3) IN ('010','011'))
)
where anio_cancel =2022
group by anio_cancel
limit 100
