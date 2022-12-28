
Select 
distinct(modems.cuenta),
modems.model_ont,
modems.p_model_ont,
modems.version_fm,
modems.vendor_ont,
intermitencia.num_intermitencia,
geografia.zona,
geografia.desc_servicio,
geografia.cluster,
geografia.distrito,
geografia.ciudad,
geografia.plaza,
geografia.region,
geografia.tipo_bundle,
geografia.familia,
geografia.fam_num,

cartera_t.fecha_activacion,
cartera_t.mes_act,
cartera_t.ano_act,
cartera_t.vida_cuenta,
cartera_t.metodo_pago,
cartera_t.morosidad,
cartera_t.score,
cartera_t.atraso,
cartera_t.estatus_cta,
cartera_t.ciclo,
cartera_t.saldo_serv,
geo_coord.geolocalizacioninstalacion__latitude__s,
geo_coord.geolocalizacioninstalacion__longitude__s,
geo_coord.planservicio__c,
geo_coord.leadsource__c,
estados.d_codigo,
estados.d_estado,
estados.indicador

from (
SELECT cuenta,
       model_ont,
       CASE
         WHEN model_ont IN ('HG8045H','HG8247','HG8245','ZTEG-824X') THEN 'antiguo'
         WHEN model_ont IN ('HG8245H','ZTEG-F660','FH-AN55060','AN5506-04-F') THEN 'seminuevo'
         WHEN model_ont IN ('HG8145V5','F660V7.0','F670LV9.0','F670LV9.0B','HG6145F','ZTEG-F670','HG8145X6') THEN 'nuevo'
       END AS p_model_ont,
       version_fm,
       vendor_ont 
     FROM bi.cta_model_ont
WHERE info_day = (SELECT MAX(info_day) FROM bi.cta_model_ont)
and p_model_ont is not null) as modems


left join(     
select cuenta ,count(*) as num_intermitencia from ( select cuenta,productid,
             to_timestamp (begintime, 'YYYYMMDDHH24MISS',true) as begin_completa,
             to_char(begin_completa,'HH24:MI:SS') as begin_hora,
             to_timestamp (endtime, 'YYYYMMDDHH24MISS',true) as end_completa,
             to_char(end_completa,'HH24:MI:SS') as end_hora,
             DATEDIFF(sec,begin_completa::timestamp,end_completa::timestamp) as segundo_transcurrido,
             substring(begintime,9,2) as hora,
             (substring(productid,2,4)*0.1)::float  as Subida_paquete,
            substring(productid,2,4)::float as Bajada_paquete,
            (Subida_paquete * segundo_transcurrido)  as total_megas_subida,
            (Bajada_paquete * segundo_transcurrido)  as total_megas_bajada
            from data_lake.edr_aaa
         where info_day between 20221001 and 20221031
         --where info_day=20221009
               and substring(begintime,1,6)::integer = 202210
                and begintime=endtime
               --and cuenta = '0113218431'
               )
               group by cuenta ) intermitencia
on modems.cuenta=intermitencia.cuenta
left join( 
    SELECT cuenta,
    A.zona,
    A.desc_servicio,
    cluster,
    distrito,
    ciudad,
    plaza,
    region, 
    tipo_bundle,
    familia, 
    case when familia='TOTALPLAY INTERNET MAS TELEVISION' then 3 else 2 end as fam_num 
    FROM 
    (select cuenta, zona, desc_servicio from black_box.cartera where info_day=20221031) A 
    LEFT JOIN 
    (SELECT "CLUSTER BRM" AS ZONA,
    "CLUSTER HOMOLOGADO" AS CLUSTER,
    distrito,
    plaza,ciudad,
    "región" as region 
    FROM bi.ds_cobertura where partition_0=202210) B 
    ON A.ZONA=B.ZONA 
    LEFT JOIN 
    (SELECT PLAN, "TIPO DE BUNDLE" AS tipo_bundle, familia FROM bi.ds_planes_homologados where partition_0=202210 )C 
    ON A.desc_servicio=C.plan) as geografia 
on modems.cuenta = geografia.cuenta
left join(
  SELECT DISTINCT 
    cuenta, 
    fecha_activacion, 
    EXTRACT(MONTH FROM CAST(fecha_activacion AS DATE)) AS mes_act, 
    EXTRACT(YEAR FROM CAST(fecha_activacion AS DATE)) AS ano_act, 
    MONTHS_BETWEEN(CURRENT_DATE, TO_DATE(fecha_activacion, 'YYYY-MM-DD')) as vida_cuenta, 
     metodo_pago, 
    morosidad, 
    score, 
    atraso, 
    estatus_cta, 
    ciclo, 
    saldo_serv -- que es saldo servido? 
    FROM black_box.cartera 
    WHERE info_day = 20221015 
   
) as cartera_t
    
on modems.cuenta=cartera_t.cuenta
left join (
select 
idcuentabrm__c,
geolocalizacioninstalacion__latitude__s,
geolocalizacioninstalacion__longitude__s,
planservicio__c,
leadsource__c
from data_staging.slf_cuentafactura__c  where info_day= 20221030) as geo_coord
on  modems.cuenta=geo_coord.idcuentabrm__c
-- estado --
left join (
SELECT cf.idcuentabrm__c, s.d_codigo, s.d_estado, i.indicador, i.mes
FROM nuevos_negocios.sepomex AS s 
     JOIN data_staging.slf_cuentafactura__c AS cf
     ON s.d_codigo=cf.codigopostalinstalacion__c
     JOIN (SELECT nom_ent, indicador, mes
           FROM data_lake.cat_inflacion_estado
           WHERE mes=10) AS i
     ON s.d_estado=i.nom_ent
WHERE cf.info_day = 20221220
) as estados
on  modems.cuenta= estados.idcuentabrm__c
where estatus_cta = 'Cerrada'

limit 100;
