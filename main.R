# 
# install.packages("uuid")  -- Instalar este packaged primeiro

library(RMySQL)
library(uuid)

##################### modificar os parametros

#working dir       # ******** modificar
wd <- '/home/agnaldo/Git/iDART/uuid_checker'   # ******** modificar
setwd(wd)
source('generic_functions.R')

# nome da us 
us_name <- 'polana_canico'                          # ******** modificar
us_referencia_pacientes <- FALSE                    # ******** modificar (TRUE apenas para CS Xipamanine, porto, bagamoio,albazine ,chamanculo,altomae)
## OpenMRS  - Configuracao de variaveis de conexao 
openmrs.user ='esaude'                         # ******** modificar
openmrs.password='esaude'                      # ******** modificar
openmrs.db.name='polana_canico'                     # ******** modificar
openmrs.host='127.0.0.1'                       # ******** modificar
openmrs.port=3306                              # ******** modificar


# Objecto de connexao com a bd openmrs
con_openmrs = dbConnect(MySQL(), user=openmrs.user, password=openmrs.password, dbname=openmrs.db.name, host=openmrs.host, port=openmrs.port)
load(file = 'data/farmac_patients.RData')

# Ignorar --> apenas para trazer pacientes da farmac
#con_postgres <-  dbConnect(PostgreSQL(),user = postgres.user,password = postgres.password, dbname = postgres.db.name,host = postgres.host)
#con.farmac <-  dbConnect(PostgreSQL(),user = postgres.user, password = postgres.password, dbname = postgres.db.name,host = postgres.host,port=postgres.port)
#farmac_patients <- dbGetQuery( con.farmac , paste0("select * from public.sync_temp_patients ;" )  )
#save( farmac_patients,file = 'data/farmac_patients.RData')



## Buscar todos pacientes OpenMRS 
openmrs_patients <- getOpenMRSPatientsWrongUuid(con_openmrs)

openmrs_patients$new_uuid <-""

# para cada paciente gerar novo uuid
sql_file_idart <- paste0('scripts/cs_',openmrs.db.name,"_idart_sql_fix_wrong_uuid.sql")
sql_file_openmrs <- paste0('scripts/cs_',openmrs.db.name,"_openmrs_sql_fix_wrong_uuid.sql")
sql_file_farmac <- paste0('scripts/cs_',openmrs.db.name,"_farmac_sql_fix_wrong_uuid.sql")
for (v in 1:nrow(openmrs_patients) ) {
    
  
  old_uuid <- openmrs_patients$uuid[v]
  nid <- openmrs_patients$identifier[v]
  new_uuid <- UUIDgenerate(use.time = TRUE, n = 1L)

  sql_update_openmrs_uuid <- paste0(" update person set uuid  ='",
                                    new_uuid,
                                          "' where uuid ='",old_uuid, "' ;")
  
  openmrs_patients$new_uuid[v] <- new_uuid
  
  sql_update_idart_uuid <-  paste0("  update  public.patient set uuidopenmrs = '",
                                   new_uuid,"' , uuid = '",   new_uuid ,"' where uuidopenmrs = '",old_uuid, "' ;")
  if(us_referencia_pacientes){
    if(nid %in% farmac_patients$patientid){
      
      write(sql_update_idart_uuid,file=sql_file_farmac,append=TRUE)
      
    }
    
  }


#write("-- ------------------------------------------------------------------------------------------------",file=sql_file_idart,append=TRUE)
write(sql_update_idart_uuid,file=sql_file_idart,append=TRUE)

#write("-- ------------------------------------------------------------------------------------------------",file=sql_file_openmrs,append=TRUE)
write(sql_update_openmrs_uuid,file=sql_file_openmrs,append=TRUE)

  
}

# salvar os logs
save(openmrs_patients, file = paste0('data/cs_',openmrs.db.name,'_openmrs_patients_wrong_uuid.RData'))
