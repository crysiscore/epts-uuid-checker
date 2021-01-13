#' Actualiza Nids na tabela Patient iDART.
#' 
#' @param con.idart conexao com  a PostgresSQL- iDART.
#' @param patient.to.update vector[id,uuid,patientid,openmrs_patient_id,full.name]  Informacao do paciente a ser actualizado.
#' @return 1, 0.
#' @examples
#' actualizaUuidIdart(con_idart, [id,uuid,patientid,openmrs_patient_id,full.name] )
#' 
actualizaUuidIdart <-   function(con.idart, patient.to.update, new.uuid) {
  
  idart <- tryCatch({
    
    message(paste0( "iDART - Actualizando uuid do paciente: ",patient.to.update[4] , " para uuid: ", new.uuid) )
    
    
    dbExecute(
      con.idart,
      paste0(
        "update  public.patient set uuidopenmrs = '",
        new.uuid,"' , uuid = '",   new.uuid ,"' where id = ",
        as.numeric(patient.to.update[1]),     " ;"
      )
    ) 
    
    msg <- paste0(' Paciente:',patient.to.update[4], ' foi actualizado com novo uuid : ', new.uuid )
    
    logAction(patient.info = patient.to.update,action = msg)
    
  },
  error = function(cond) {
    
    msg <-  paste0(" BD_ERROR - Error iDART - public.patient Nao foi possivel Actualizar o uuid do paciente:  ",patient.to.update[4], 
                   "- ",paste0(patient.info[5],' ', patient.info[6]), " Para : ", new.nid, " Erro: ")
    
    #message(msg) imprimir a mgs a consola
    logAction(patient.info = patient.to.update,action = msg)
    # Choose a return value in case of error
    return(FALSE)
  },
  finally = {
    # NOTE:
    # Here goes everything that should be executed at the end,
    # regardless of success or error.
    # If you want more than one expression to be executed, then you
    # need to wrap them in curly brackets ({...}); otherwise you could
    # just have written 'finally=<expression>'
    
  })
  
  idart
  
}




#' Escreve  os logs das accoes executadas nas DBs iDART/OpenMRS numa tabela logsExecucao
#' 
#' @param patient.info informacao do paciente[id,uuid,patientid,openmrs_patient_id,full.name,index]   
#' @param action descricao das accoes executadas sobre o paciente
#' @return append uma row na tabela logs
#' @examples
#' logAction(patientToUpdate, ' Paciente com NID X Alrerado Para NID Y')
logAction <- function (patient.info,action){
  
  
  logsExecucao <<-  add_row(logsExecucao, id=as.numeric(patient.info[1]),uuid=patient.info[3],patientid=patient.info[4],
                            full_name= paste0(patient.info[5],' ', patient.info[6]), accao=action)
  # logsExecucao <<- rbind.fill(logsExecucao, temp)  gera registos multipos no log
  
}




#' Compoe um vector com dados do paciente que se vai actualizar o uuid
#' 
#' @param df tabela de pacientes com uuid incorecto 
#' @param index row do paciente em causa 
#' @return vector[id,uuid,uuidopenmrs, patientid,fname,lname,index] 
#' @examples composePatientToUpdate(67, df_uuid)
composePatientToUpdate <- function(index,df){
  
  id = df$id[index]
  uuidopenmrs = df$uuidopenmrs[index]
  uuid = df$uuid[index]
  patientid = df$patientid[index]
  fname =  df$firstnames[index]
  lname =  df$lastname[index]
  Encoding(fname) <- "latin1"
  Encoding(lname) <- "latin1"
  fname  <- iconv(fname, "latin1", "UTF-8",sub='')
  lname  <- iconv(lname, "latin1", "UTF-8",sub='')
  patient <- c(id,uuid,uuidopenmrs, patientid,fname,lname,index)
  patient
}






#' Busca todos pacientes do OpenMRS
#' 
#' @param con.postgres  obejcto de conexao com BD OpenMRS
#' @return tabela/dataframe/df com todos paciente do OpenMRS 
#' @examples patients_idart <- getAllPatientsIdart(con_openmrs)
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

