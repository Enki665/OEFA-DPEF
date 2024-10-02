# install.packages("dplyr")
# install.packages("googlesheets4")
# install.packages("tidyverse")
# install.packages("xlsx")
library(dplyr)
library(googlesheets4)
library(tidyverse)
library(xlsx)
#BASES DE DATOS 
#DATOS DE LA CONSULTA DEL APLICATIVO SIREFA 
SIREFA <- read_sheet("1q39TS9bsEBg8hzOlCkFRmqmGQvt2FjavGppQvrULJQk", sheet = "SIREFA", col_types = "c")
#DATOS DE LA CONSULTA DEL APLICATIVO SIGIP 
CONSULTA_SIGIP <- read_sheet("1QxhvStYckMuhnkPKu5wkygHsAKsvwo1cZVR63u-QB2g",sheet = "CONSOLIDADO", col_types = "c")

#AGRUPAMOS LOS NOMBRES POR TIPO DE EFA 

CONSULTA_SIGIP_1 <- CONSULTA_SIGIP %>%
  select(`Cod de EFA`, EFA, DEPARTAMENTO, PROVINCIA, DISTRITO,RUC)%>%
  unique()%>%
  group_by(`Cod de EFA`)%>%
  mutate( Cantidad = n()) %>%
  filter(Cantidad == 2)
  
#Vemos las diferencias entre SIGIP y SIREFA 
#Se realizará el cruce de información por el número RUC de las EFA

#Seleccionamos la data necesaria para el cruce de información de las EFA del SIREFA 
SIREFA_1 <- SIREFA %>%
  mutate_if(is.character, toupper)%>%
  filter(NU_ESTADO == 1) %>%
  select(TX_RUC, TX_NOMBRE) %>%
  rename( RUC = TX_RUC,
          SIREFA_NOMBRE = TX_NOMBRE)

#Seleccionamos la data necesaria para el cruce de información de las EFA del SIGIP 
CONSULTA_SIGIP_2 <- CONSULTA_SIGIP %>%
  mutate_if(is.character, toupper) %>%
  select(`Cod de EFA`, EFA, DEPARTAMENTO, PROVINCIA, DISTRITO,RUC)%>%
  unique()


SIREFA_SIGIP <- left_join(SIREFA_1, CONSULTA_SIGIP_2, by = "RUC") %>% 
  mutate(COMPARACION_NOMBRES = case_when(
    EFA == SIREFA_NOMBRE ~ "CORRECTO",  
    TRUE ~ "OBSERVADO"                  
  )) %>%
  filter(COMPARACION_NOMBRES == "OBSERVADO")


#ESCRIBIMOS EL REPORTE ENCONTRADO EN LA PAGÍNA GOOGLE SHEETS 
#Inconsistencias en base al RUC que tienen las EFA
write_sheet(CONSULTA_SIGIP_1,"1iOIlG1KylUTzT-F9WGtqOUkts3H5C8_odzy9ZrVdJkY" ,sheet = "SIGIP")

#Inconsistencias respecto al nombre de las EFA 
write_sheet(SIREFA_SIGIP,"1iOIlG1KylUTzT-F9WGtqOUkts3H5C8_odzy9ZrVdJkY" ,sheet = "SIREFA-SIGIP")

  
  
  
  
  
  

