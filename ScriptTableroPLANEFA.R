
#LIBRERIAS 
library(googlesheets4)
library(dplyr)
#NOS CONECTAMOS A LAS BASES DE DATOS 
library(googlesheets4)
library(dplyr)

# Definimos la Función para procesar los campos necesarios de cada base de datos
procesar_efa <- function(sheet_id, sheet_name, anio, skip_rows = 2) {
  read_sheet(sheet_id, sheet_name, col_types = "c", skip = skip_rows) %>%
    mutate_if(is.character, toupper) %>%
    select(EFA, `TIPO DE EFA`, `Cod de EFA`, Departamento, Provincia, Distrito, `OD competente`, `Clasificación MEF`) %>%
    rename(NOMBRE_EFA = EFA,
           TIPO_EFA = `TIPO DE EFA`,
           COD_EFA = `Cod de EFA`,
           DEPARTAMENTO = Departamento,
           PROVINCIA = Provincia,
           DISTRITO = Distrito,
           OD_COMPETENTE = `OD competente`,
           CLASIFICACION_MEF = `Clasificación MEF`) %>%
    mutate(ANIO = as.character(anio))
}

# Aplicación de la función para cada año
EFAS_2019 <- procesar_efa("16c1RZPx4Rf2thnMYTwlQQuQW3qO6Ci68SPVb3kiTAa0", "2019", 2019)
EFAS_2020 <- procesar_efa("16c1RZPx4Rf2thnMYTwlQQuQW3qO6Ci68SPVb3kiTAa0", "2020", 2020)
EFAS_2021 <- procesar_efa("1LqAldAIDBoalAMkNN5bfDapzb0KbwzeTu1YJJUe4CUs", "PLANEFA 2021", 2021)
EFAS_2022 <- procesar_efa("1YMApKOdkucbO8QZEOYLtocxca1qhfcdZKOshZfuHpK0", "PLANEFA 2022", 2022)
EFAS_2023 <- procesar_efa("1GO2FDal3DCcdyHOkkDzy3PTPD55m1iZxKkl8icODBRQ", "PLANEFA 2023", 2023, skip_rows = 0)
EFAS_2024 <- procesar_efa("1LnHCutQUhideDlC5X53ucGbHSB9OHzbozGSlF-6Q1cQ", "PLANEFA 2024", 2024, skip_rows = 0)
EFAS_2025 <- procesar_efa("1y8cATboaQGxwpM-JvyJclcIAi9sJle7XT_Si1pXfzwc", "PLANEFA 2025", 2025, skip_rows = 0)


#COMBINAMOS LAS EFAS POR AÑO EN UN ÚNICO DATASET 
LISTA_EFAS <- rbind(EFAS_2019,EFAS_2020,EFAS_2021,EFAS_2022,EFAS_2023,EFAS_2024,EFAS_2025)
LISTA_EFAS_1 <- LISTA_EFAS %>%
  mutate(PK = paste(COD_EFA,ANIO))
write_sheet(LISTA_EFAS_1,"1UKxisf7NY_C7meG5j5XuynanVJsFYxgrAkiuh_Pjmvs","LISTA_EFAS")
#........................................................................................................................
#..........................PRESENTACIÓN PLANEFA EN EL APLICATIVO SIGIP ..................................................
#........................................................................................................................
#Se consume la información que se actualiza diariamente con los SQL en la carpeta drive 
#01.PRESENTACION PLANEFA

PLANEFA_2021_2025 <- read_sheet("1QxhvStYckMuhnkPKu5wkygHsAKsvwo1cZVR63u-QB2g", "PLANEFA", col_types = "c")
PLANEFA_2015_2020 <- read_sheet("1QxhvStYckMuhnkPKu5wkygHsAKsvwo1cZVR63u-QB2g", "Histórico PLANEFA", col_types = "c")
#02. SELECCIONAMOS LA DATA A UTILIZAR 
PLANEFA_2021_2025_1 <- PLANEFA_2021_2025 %>%
  mutate_if(is.character,toupper)%>%
  select(ANIO,ID_ADMINISTRADO, NOMBRE_EFA, FLAG_ENVIADO, FECHA_ENVIO, FECHA_DOC_APROBACION_PLANEFA,CON_ANEXO_UNICO) %>%
  mutate(FECHA_ENVIO = as.Date(FECHA_ENVIO, format="%d/%m/%Y"),
         FECHA_APROB_PLANEFA = as.Date(FECHA_DOC_APROBACION_PLANEFA, format="%d/%m/%Y"),
         PRESENTACION_PLANEFA = case_when(is.na(FLAG_ENVIADO) ~ "PENDIENTE", TRUE ~ "PRESENTADO"),
         PK = paste(ID_ADMINISTRADO,ANIO))
         
PLANEFA_2015_2020_1 <- PLANEFA_2015_2020 %>%
  mutate_if(is.character,toupper)%>%
  filter(ANIO %in% c("2019","2020"))%>%
  select(ANIO,ID_ADMINISTRADO, NOMBRE_EFA, FLAG_ENVIADO, FECHA_ENVIO, FECHA_DOC_APROBACION_PLANEFA,CON_ANEXO_UNICO) %>%
  mutate(FECHA_ENVIO = as.Date(FECHA_ENVIO, format="%d/%m/%Y"),
         FECHA_APROB_PLANEFA = as.Date(FECHA_DOC_APROBACION_PLANEFA, format="%d/%m/%Y"),
         PRESENTACION_PLANEFA = case_when(is.na(FLAG_ENVIADO) ~ "PENDIENTE", TRUE ~ "PRESENTADO"),
         PK = paste(ID_ADMINISTRADO,ANIO))

#Combinamos los dos dataset con información del 2019 al 2025

PRESENTACION_PLANEFA <- rbind(PLANEFA_2015_2020_1,PLANEFA_2021_2025_1)

#CRUZAMOS LA DATA DEL SIGIP CON LAS EFAS 

PRESENTACION_PLANEFA_TOTAL <- left_join(LISTA_EFAS_1,PRESENTACION_PLANEFA, by="PK")

#ESCRIBIMOS LA DATA EN LA HOJA DE GOOGLE_SHEETS
write_sheet(PRESENTACION_PLANEFA_TOTAL,"1UKxisf7NY_C7meG5j5XuynanVJsFYxgrAkiuh_Pjmvs","PRESENTACION_PLANEFA")

#........................................................................................................................
#..........................EVALUACIONES PLANEFA EN EL APLICATIVO SIGIP(reportes trimestrales) ...........................
#........................................................................................................................
#Conectamos a las bases de datos que se actualizan diariamente
EVALUACIONES_2019_2020 <- read_sheet("135xoTjT2Ro5FHHBCcmKY9ZiPMAsYncplSz8BQ-O7moM","Histórico Evaluación", col_types = "c")
EVALUACIONES_2021_2025 <- read_sheet("135xoTjT2Ro5FHHBCcmKY9ZiPMAsYncplSz8BQ-O7moM", "Evaluación", col_types = "c")

#Combinamos las bases de datos en una sola
EVALUACIONES_TOTALES <- rbind(EVALUACIONES_2019_2020,EVALUACIONES_2021_2025)
#Asignamos el formato necesario para las operaciones del tablero
EVALUACIONES_TOTALES_1 <- EVALUACIONES_TOTALES%>%
  mutate_if(is.character,toupper) %>%
  mutate(PROG_TRIMESTRAL = as.numeric(PROG_TRIMESTRAL),
         EJECUCION_TRIMESTRAL = as.numeric(EJECUCION_TRIMESTRAL),
         ACC_EJEC_NO_PROG = as.numeric(ACC_EJEC_NO_PROG),
         PRESUPUESTO_EJECUTADO_TRIM = as.numeric(PRESUPUESTO_EJECUTADO_TRIM))

#Escribimos el total de evaluaciones realizadas en el drive
write_sheet(EVALUACIONES_TOTALES_1,"1UKxisf7NY_C7meG5j5XuynanVJsFYxgrAkiuh_Pjmvs","EVALUACIONES")

#........................................................................................................................
#..........................SUPERVISIONES PLANEFA EN EL APLICATIVO SIGIP(reportes trimestrales)...........................
#........................................................................................................................

#Conectamos a las bases de datos que se actualizan diariamente
SUPERVISIONES_2019_2020 <- read_sheet("1Gw543SjPZX9K1xR9TC8wuqe99IPDzRYsSOd7VuBDk90","Histórico", col_types = "c")
SUPERVISIONES_2021_2025 <- read_sheet("1Gw543SjPZX9K1xR9TC8wuqe99IPDzRYsSOd7VuBDk90", "Supervisión", col_types = "c")

#Combinamos las bases de datos en una sola 

SUPERVISIONES_TOTALES <- rbind(SUPERVISIONES_2019_2020,SUPERVISIONES_2021_2025)

#Asignamos el formato necesario para las operaciones del cálculo
SUPERVISIONES_TOTALES_1 <- SUPERVISIONES_TOTALES %>%
  mutate(PROG_TRIMESTRAL = as.numeric(PROG_TRIMESTRAL),
         EJECUCION_TRIMESTRAL = as.numeric(EJECUCION_TRIMESTRAL),
         ACC_EJEC_NO_PROG = as.numeric(ACC_EJEC_NO_PROG),
         PRESUPUESTO_EJEC = as.numeric(PRESUPUESTO_EJEC))

#Escribimos el total de supevisiones realizadas en el drive
write_sheet(SUPERVISIONES_TOTALES_1, "1UKxisf7NY_C7meG5j5XuynanVJsFYxgrAkiuh_Pjmvs", "SUPERVISIONES")

#........................................................................................................................
#..........................INSTRUMENTOS NORMATIVOS PLANEFA EN EL APLICATIVO SIGIP(reportes trimestrales)...........................
#........................................................................................................................

#Conectamos a la data

IN_2019_2020 <- read_sheet("1SH3jAlD_Z1CKRNYcaiiqKHy1_OHX9jSqyM6-MtM7-0U","Histórico", col_types = "c")
IN_2021_2025 <- read_sheet("1SH3jAlD_Z1CKRNYcaiiqKHy1_OHX9jSqyM6-MtM7-0U","Instrumentos Normativos", col_types = "c")

#Combinamos la data en una sola 
IN_TOTAL <- rbind(IN_2019_2020,IN_2021_2025)

IN_TOTAL_1 <- IN_TOTAL %>%
  mutate(PROG_TRIMESTRAL = as.numeric(PROG_TRIMESTRAL),
         EJECUCION_TRIMESTRAL = as.numeric(EJECUCION_TRIMESTRAL),
         ACC_EJEC_NO_PROG = as.numeric(ACC_EJEC_NO_PROG),
         PRESUPUESTO_EJECUTADO_TRIM = as.numeric(PRESUPUESTO_EJECUTADO_TRIM))

#Escribimos el total de instrumentos normativos
write_sheet(IN_TOTAL_1,"1UKxisf7NY_C7meG5j5XuynanVJsFYxgrAkiuh_Pjmvs","INSTRUMENTOS_NORMATIVOS")


#........................................................................................................................
#..........................PAS PLANEFA EN EL APLICATIVO SIGIP(reportes trimestrales)....................................
#........................................................................................................................


#Nos conectamos a la data 

PAS_2021_2024_EJEC <- read_sheet("11cKS8IUiDTRQXRu1kFzN4xyzh1vhJJYAbSIZmC-x_Kc","PAS - Ejecutadas", col_types = "c")
PAS_2019_2020_EJEC <- read_sheet("11cKS8IUiDTRQXRu1kFzN4xyzh1vhJJYAbSIZmC-x_Kc","Histórico PAS - Ejecutadas", col_types = "c")
PAS_2021_2024_NOEJEC <- read_sheet("11cKS8IUiDTRQXRu1kFzN4xyzh1vhJJYAbSIZmC-x_Kc","PAS - No Ejecutadas", col_types = "c")
##PAS_2019_2021_NOEJEC <- read_sheet("11cKS8IUiDTRQXRu1kFzN4xyzh1vhJJYAbSIZmC-x_Kc","PAS - No Ejecutadas", col_types = "c")#Revisar dataset/información inconsistente
#Presenta 301691 registros, un valor atípico en base a otros años


#SELECCIONAMOS LA DATA A UTILIZAR DE CADA DATASET 

PAS_2021_2024_EJEC_1 <- PAS_2021_2024_EJEC %>%
  mutate_if(is.character,toupper) %>%
  select(ANIO,ID_ADMINISTRADO,NOMBRE,DEPARTAMENTO,PROVINCIA,DISTRITO,TRIMESTRE,RUC,CLASIFICACION_MEF,OD_COMPETENTE,
         NIVEL_GOBIERNO,NRO_INFORME_SUPERVISION,NRO_EXPEDIENTE,ADMINISTRADO,NUMERO_DOCUMENTO_ADM,FECHA_NOTIF_INICIO_PAS,
         NRO_RESOLUCION_PAS,FECHA_RES_CULMINA_PAS,SENTIDO_RESOLUCION,MONTO_SANCION_UIT,SE_DICTARON_MEDIDAS_ADMINIS)%>%
  mutate(SE_EJECUTO_PAS ="SI",
         MONTO_SANCION_UIT = as.numeric(MONTO_SANCION_UIT),
         FECHA_NOTIF_INICIO_PAS = as.Date(FECHA_NOTIF_INICIO_PAS,format="%d/%m/%Y"),
         FECHA_RES_CULMINA_PAS = as.Date(FECHA_RES_CULMINA_PAS, format="%d/%m/%Y"))

PAS_2019_2020_EJEC_1 <- PAS_2019_2020_EJEC %>%
  mutate_if(is.character,toupper) %>%
  select(ANIO,ID_ADMINISTRADO,NOMBRE,DEPARTAMENTO,PROVINCIA,DISTRITO,TRIMESTRE,RUC,CLASIFICACION_MEF,OD_COMPETENTE,
         NIVEL_GOBIERNO,NRO_INFORME_SUPERVISION,NRO_EXPEDIENTE,ADMINISTRADO,NUMERO_DOCUMENTO_ADM,FECHA_NOTIF_INICIO_PAS,
         NRO_RESOLUCION_PAS,FECHA_RES_CULMINA_PAS,SENTIDO_RESOLUCION,MONTO_SANCION_UIT,SE_DICTARON_MEDIDAS_ADMINIS)%>%
  mutate(SE_EJECUTO_PAS ="SI",
         MONTO_SANCION_UIT = as.numeric(MONTO_SANCION_UIT),
         FECHA_NOTIF_INICIO_PAS = as.Date(FECHA_NOTIF_INICIO_PAS,format="%d/%m/%Y"),
         FECHA_RES_CULMINA_PAS = as.Date(FECHA_RES_CULMINA_PAS, format="%d/%m/%Y"))


PAS_2021_2024_NOEJEC_1 <- PAS_2021_2024_NOEJEC %>%
  mutate_if(is.character,toupper) %>%
  select(ANIO,ID_ADMINISTRADO,NOMBRE,DEPARTAMENTO,PROVINCIA,DISTRITO,TRIMESTRE,RUC,CLASIFICACION_MEF,OD_COMPETENTE,
         NIVEL_GOBIERNO)%>%
  mutate(SE_EJECUTO_PAS = "NO",
         NRO_INFORME_SUPERVISION = "",
         NRO_EXPEDIENTE= "",
         ADMINISTRADO= "",
         NUMERO_DOCUMENTO_ADM= "",
         FECHA_NOTIF_INICIO_PAS= as.Date("",format="%d/%m/%Y"),
         NRO_RESOLUCION_PAS= "",
         FECHA_RES_CULMINA_PAS= as.Date("",format="%d/%m/%Y"),
         SENTIDO_RESOLUCION= "",
         MONTO_SANCION_UIT= "",
         SE_DICTARON_MEDIDAS_ADMINIS= "")

#Combinamos las bases de datos 

PAS_TOTAL <- rbind(PAS_2019_2020_EJEC_1,PAS_2021_2024_EJEC_1,PAS_2021_2024_NOEJEC_1)

#Escribimos el total de procedimientos administrativos 
write_sheet(PAS_TOTAL,"1UKxisf7NY_C7meG5j5XuynanVJsFYxgrAkiuh_Pjmvs","PAS")


