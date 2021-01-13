library(RMySQL)
source('generic_functions.R')
# install.packages("uuid")  -- Instalar este packaged primeiro
library(uuid)


#working dir
wd <- '/home/agnaldo/Git/iDART/uuid_checker'
setwd(wd)


## OpenMRS  - Configuracao de variaveis de conexao 
openmrs.user ='esaude'                     # ******** modificar
openmrs.password='esaude'                  # ******** modificar
openmrs.db.name='bagamoio'                 # ******** modificar
openmrs.host='127.0.0.1'                   # ******** modificar
openmrs.port=3306                          # ******** modificar


## iDART  - Configuracao de variaveis de conexao 
postgres.user ='postgres'                      # ******** modificar
postgres.password='iD@rt2020'                   # ******** modificar
postgres.db.name='pharm'                  # ******** modificar
postgres.host='192.168.1.163'                     # ******** modificar
postgres.port=5432                             # ******** modificar

# Objecto de connexao com a bd openmrs
con_openmrs = dbConnect(MySQL(), user=openmrs.user, password=openmrs.password, dbname=openmrs.db.name, host=openmrs.host, port=openmrs.port)
#con_postgres <-  dbConnect(PostgreSQL(),user = postgres.user,password = postgres.password, dbname = postgres.db.name,host = postgres.host)
#con.farmac <-  dbConnect(PostgreSQL(),user = postgres.user, password = postgres.password, dbname = postgres.db.name,host = postgres.host,port=postgres.port)

load(file = 'data/farmac_patients.RData')
#farmac_patients <- dbGetQuery( con.farmac , paste0("select * from public.sync_temp_patients ;" )  )
#save( farmac_patients,file = 'data/farmac_patients.RData')

## Pacientes
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
  if(nid %in% farmac_patients$patientid){
    write(sql_update_idart_uuid,file=sql_file_farmac,append=TRUE)
    
  }

#write("-- ------------------------------------------------------------------------------------------------",file=sql_file_idart,append=TRUE)
write(sql_update_idart_uuid,file=sql_file_idart,append=TRUE)

#write("-- ------------------------------------------------------------------------------------------------",file=sql_file_openmrs,append=TRUE)
write(sql_update_openmrs_uuid,file=sql_file_openmrs,append=TRUE)

  
}



