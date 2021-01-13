
#' Busca todos pacientes do OpenMRS com uuid mal formatados
#' 
#' @param con.openmrs  obejcto de conexao com BD OpenMRS
#' @return tabela/dataframe/df com todos paciente do OpenMRS 
#' @examples patients_wrong_uuid <- getOpenMRSPatientsWrongUuid(con_openmrs)
getOpenMRSPatientsWrongUuid <- function(con.openmrs) {
  rs  <-
    dbSendQuery(
      con.openmrs,
      paste0(
        " SELECT   
          pat.patient_id, 
          pid.identifier , 
          pe.uuid,
          pe.birthdate,
           pn.given_name  given_name ,
           pn.middle_name middle_name,
           pn.family_name  family_name,
          concat(lower(pn.given_name),if(lower(pn.middle_name) is not null,concat(' ', lower(pn.middle_name) ) ,''),concat(' ',lower(pn.family_name)) ) as full_name_openmrs ,
          estado.estado as estado_tarv ,
          max(estado.start_date) data_estado,
          date(visita.encounter_datetime) as data_ult_consulta,
          date(visita.value_datetime) as data_prox_marcado
          FROM  patient pat INNER JOIN  patient_identifier pid ON pat.patient_id =pid.patient_id and pat.voided=0 
          INNER JOIN person pe ON pat.patient_id=pe.person_id and pe.voided=0 
          INNER JOIN person_name pn ON pe.person_id=pn.person_id and    pn.voided=0
          LEFT JOIN
        		(
        			SELECT 	pg.patient_id,ps.start_date encounter_datetime,location_id,ps.start_date,ps.end_date,
        					CASE ps.state
                                WHEN 6 THEN 'ACTIVO NO PROGRAMA'
        						WHEN 7 THEN 'TRANSFERIDO PARA'
        						WHEN 8 THEN 'SUSPENSO'
        						WHEN 9 THEN 'ABANDONO'
        						WHEN 10 THEN 'OBITO'
                                WHEN 29 THEN 'TRANSFERIDO DE'
        					ELSE 'OUTRO' END AS estado
        			FROM 	patient p
        					INNER JOIN patient_program pg ON p.patient_id=pg.patient_id
        					INNER JOIN patient_state ps ON pg.patient_program_id=ps.patient_program_id
        			WHERE 	pg.voided=0 AND ps.voided=0 AND p.voided=0 AND
        					pg.program_id=2 AND ps.state IN (6,7,8,9,10,29) AND ps.end_date IS NULL
        
        
        		) estado ON estado.patient_id=pe.person_id
         LEFT Join
             (
        		Select ult_levantamento.patient_id,ult_levantamento.encounter_datetime,o.value_datetime
        		from
        
        			(	select 	p.patient_id,max(encounter_datetime) as encounter_datetime
        				from 	encounter e
        						inner join patient p on p.patient_id=e.patient_id
        				where 	e.voided=0 and p.voided=0 and e.encounter_type in (6,9)
        				group by p.patient_id
        			) ult_levantamento
        			left join encounter e on e.patient_id=ult_levantamento.patient_id
        			left join obs o on o.encounter_id=e.encounter_id
        			where o.concept_id=1410 and o.voided=0 and e.encounter_datetime=ult_levantamento.encounter_datetime and
        			e.encounter_type in (6,9)
        		) visita  on visita.patient_id=pn.person_id
            where length(pe.uuid) <> 36
             group by pat.patient_id order by     pat.patient_id
        
                "
      )
    )
  
  data <- fetch(rs, n = -1)
  RMySQL::dbClearResult(rs)
  return(data)
  
}

